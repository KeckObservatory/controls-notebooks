global log
import logging
FORMAT = '%(asctime)s (%(levelname)s) %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
log = logging.getLogger(__name__)

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import json
import urllib.request
import urllib.parse
import requests
import datetime
import dateutil.parser
import pandas as pd
import traceback
import math

# Demonstration data
demo_data = pd.DataFrame({
    'motor': ['2A', '2B', '2B', '2B', '2B', '2B', '2B', '3A', '3A', '3A', 
              '3B', '3B', '3B', '3B', '3B', '3B', '3B', '3B', '3B', '3B', 
              '3B', '3B', '3B', '3B', '3B', '3B', '3B', '3B', '3B', '3B', 
              '3B', '4B'],
    'pos':   [288, 155, 156, 157, 289, 255, 355, 339, 142, 25, 
              339, 355, 354, 233, 332, 350, 124, 257, 138, 139, 
              140, 135, 141, 142, 143, 144, 145, 146, 25, 23, 
              11, 184],
    'count': [1, 1, 2, 1, 4, 1, 1, 3, 1, 1, 
              13, 1, 1, 4, 1, 1, 1, 1, 1, 2, 
              3, 1, 1, 1, 5, 4, 1, 2, 1, 1, 
              1, 1]
})  

def Setup():
    # Logging format
    #FORMAT = '%(asctime)s (%(levelname)s) %(message)s'
    #logging.basicConfig(level=logging.DEBUG, format=FORMAT)
    #global log
    #globals()['log'] = logging.getLogger(__name__)
    #log = logging.getLogger(__name__)
    
    # 2 telescopes
    global tels
    tels = [1, 2]
    
    # Archiver hostname and port
    global archiver
    archiver = 'k{}epicsgateway'

    global port
    port = 17668
    
    # Enumerate how many motors to look for slips, such as kN:dcs:axe:az:mtr1ASlip
    global motors
    motors = ['1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B']
        
    log.info('Slip report initialized.')
    

# -------------------------------------------------------------------------------------------------
# Interface to the archiver to retrieve a set of data
def GetSlips(archiver, pv_pos, pv_motor_slip, motor, t0, t1):

    # Convert the date stamps
    start = t0.astimezone().isoformat()
    end = t1.astimezone().isoformat()

    # -----------------------------------------------------------------------------
    # Get the slip data for this motor

    # Build the URL to retrieve the data from the server
    path = 'http://{}:{}/retrieval/data/getData.json?{}'
    parm = urllib.parse.urlencode({'pv': pv_motor_slip, 'from': start, 'to': end})
    url = path.format(archiver, port, parm)

    # Retrieve the raw values from the archiver web server
    with urllib.request.urlopen(url) as f:
        dat = f.read().decode('utf-8')

    # Convert the JSON into arrays
    x = json.loads(dat)

    # Check for nothing coming back
    if len(x) == 0:
        log.info(f'No slip data returned for {pv_motor_slip} at {start}')
        exit(1)

    # Extract the slip times and values
    count = 0

    slips = []
    for d in x[0]['data']:
        val = d['val']
        sampletime = pd.Timestamp(d['secs'] + d['nanos'] / 10e9, unit='s')

        # We only care about the 1 values, which indicates a slip has occurred, not the 0 which is the
        # lowering of the fault flag
        if val == 1:
            slips.append((sampletime, val))
            count += 1

    # Only continue if there were any slips found
    if count == 0:
        log.warning(f'No motor slips found for {pv_motor_slip} at {start}')
        return pd.DataFrame([], columns=['sampletime', 'motor', 'pos'])

    # -----------------------------------------------------------------------------
    # Get the AZ position of each slip
    temp = []
    for sampletime, _ in slips:

        # Build the URL to retrieve the data from the server
        t = sampletime.isoformat() + '-10:00'
        parm = urllib.parse.urlencode({'at': t})
        url = f'http://{archiver}:{port}/retrieval/data/getDataAtTime.json?{parm}'
        payload = [pv_pos]
        headers = {'content-type': 'application/json'}

        # print(f'Get data: {url}')
        response = requests.post(url, data=json.dumps(payload), headers=headers)

        # Convert the JSON into arrays
        try:
            pos = json.loads(response.content)[pv_pos]['val']
            pos = math.floor(pos)  # do not need fractional degrees

            # Normalize to positive degree values
            if pos < 0:
                pos += 360

        except Exception as e:
            log.critical(f'Unable to retrieve motor slip position!  URL = {url}  {e}')
            exit(1)

        temp.append((sampletime, motor, pos))

    # Build a new dataframe to contain the retrieved values
    df = pd.DataFrame(temp, columns=['sampletime', 'motor', 'pos'])
    return df


# -------------------------------------------------------------------------------------------------
# Wrapper around retrieving data from the archivers
def GetAllSlips(telescope, t0, t1):

    # Results go into a dataframe
    results = pd.DataFrame(columns=['sampletime', 'motor', 'pos'])

    # Start the processing
    log.info(f'Motor slip analysis for {telescope}: {t0} to {t1}')

    # Check the telescope parameter
    if telescope not in [0,1]:
        log.critical('Specify telescope as either 0 or 1.')
        return results

    # EPICS record names for our data
    pv_motor_slip_template = 'k{}:dcs:axe:az:mtr{}Slip'
    pv_pos_template = 'k{}:dcs:axe:az:cepDeg'

    try:
        for motor in motors:
            pv_motor_slip = pv_motor_slip_template.format(telescope, motor)
            pv_pos = pv_pos_template.format(telescope)

            # Get the slip events for this motor
            df = GetSlips(archiver.format(telescope), pv_pos, pv_motor_slip, motor, t0, t1)

            # Append to the other results
            results = pd.concat([results, df])

    except Exception as e:
        log.info(f'Exception during processing: {e}')
        log.info(traceback.format_exc())
    finally:
        log.info('Check complete.')

    return results

# -------------------------------------------------------------------------------------------------
# Count the number of slips per degree
def ReduceSlipData(df):
    # Make a new results data frame that drops the timestamp data but creates a count for each 
    # degree of az, once per motor
    reduced = pd.DataFrame(columns=['motor', 'pos', 'count'])
    
    # Iterate each row of the data frame and build up the counts
    for index, row in df.iterrows():
        motor = row['motor']
        pos = row['pos']
        
        # See if we already have an entry for this motor/position
        match = reduced[(reduced['motor'] == motor) & (reduced['pos'] == pos)]
        
        if match.empty:
            # New entry
            reduced = pd.concat([reduced, pd.DataFrame({'motor': [motor], 'pos': [pos], 'count': [1]})], ignore_index=True)
        else:
            # Increment the count
            idx = match.index[0]
            reduced.at[idx, 'count'] += 1

    return reduced        



# -------------------------------------------------------------------------------------------------
# Plot the slips
from bokeh.plotting import figure, output_file, save, show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.palettes import Plasma11 as palette
from bokeh.io import output_notebook
output_notebook()

def PlotSlips(reduced, flip=True, top=-90):

    rows = {'start_angle': [], 'end_angle': [], 
            'inner': [], 'outer': [], 
            'color': [], 'alpha': [], 
            'legs' : [], 'motor': [], 'pos': [], 'count': [], 'name': []}

    additional_radius = {'1A': 3, '1B': 2, '2A': 1.5, '2B': 1.5, '3A': 1, '3B': 1, '4A': 0.75, '4B': 0.75}

    for row in reduced.itertuples():
        
        # Use the motor name to select a color from the pallette
        motor_index = motors.index(row.motor)
        
        # Use the count to select a color from the pallette
        if row.count < 10:
            color_index = row.count  # scale count to palette index
        else:
            color_index = 10  # max index

        color = palette[color_index]

        # Flip the azimuth position if requested
        pos = int(row.pos)
        if flip:
            flipped_pos = (360 - int(row.pos)) % 360
        else:
            flipped_pos = int(row.pos) % 360

        # Rotate so top is at specified angle
        flipped_pos = (flipped_pos - top) % 360

        count = int(row.count)
        start = math.radians(flipped_pos)

        # Make the wedge narrower for the outer rings
        end = math.radians(((flipped_pos + additional_radius[row.motor])) % 360)

        # If end <= start (wrap), add 2*pi so wedge draws correctly
        if end <= start:
            end = end + 2 * math.pi

        # Let alpha stay at 1 for now
        alpha = 1

        rows['start_angle'].append(start)
        rows['end_angle'].append(end)
        
        # Set the inner/outer radius based on motor
        inner_radius = 0.1 + (motor_index * 0.1)

        # Add extra to the outer radius when the count is high
        if count >= 10:
            outer_radius = inner_radius + 0.15
        elif count >= 5:
            outer_radius = inner_radius + 0.13
        elif count >= 3:
            outer_radius = inner_radius + 0.12
        else:
            outer_radius = inner_radius + 0.1

        rows['inner'].append(inner_radius)
        rows['outer'].append(outer_radius)

        rows['color'].append(color)
        rows['alpha'].append(alpha)
        rows['motor'].append(row.motor)
        rows['pos'].append(pos)
        rows['count'].append(count)
        rows['name'].append('testing')

        # Compute where the telescope legs are for this azimuth
        leg1 = (pos + 45) % 360
        leg2 = (leg1 + 90) % 360
        leg3 = (leg2 + 90) % 360
        leg4 = (leg3 + 90) % 360

        deg = chr(176)
        rows['legs'].append(f'{leg1}{deg} {leg2}{deg} {leg3}{deg} {leg4}{deg}')

    source = ColumnDataSource(rows)

    p = figure(width=700, height=700, title='Slip counts by Motor and Azimuth (360°)', tools='', x_range=(-1, 1), y_range=(-1, 1))
    annularWedgeRenderer = p.annular_wedge(x=0, y=0, 
                    direction='anticlock',
                    inner_radius='inner', outer_radius='outer', 
                    start_angle='start_angle', end_angle='end_angle',                     
                    color='color', alpha='alpha', line_color='white', 
                    source=source)

    # Add hover tool for the slips
    hover = HoverTool(renderers=[annularWedgeRenderer], tooltips=[('Motor', '@motor'), ('Azimuth', '@pos°'), ('Count', '@count'), ('Legs', '@legs')])
    p.add_tools(hover)

    # Create a circle for each of the motors, 8 total
    for radius in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        p.arc(x=0, y=0, radius=radius, start_angle=0, end_angle=math.radians(360), color="navy")

    # Annotate the motor rings
    for i, motor in enumerate(motors):
        radius = 0.1 + (i * 0.1) + 0.035
        angle_rad = math.radians((0 - top) % 360)
        x = radius * math.cos(angle_rad)
        y = radius * math.sin(angle_rad)
        p.text(x=[x], y=[y], text=[f'{motor[0]}  {motor[1]}'], text_align='center', 
               text_baseline='middle', text_color='red', text_font_size='11pt', alpha=0.5)

    # Indicate where 0° is, rotated to top, convert the top angle to radians
    top_rad = math.radians((0 - top) % 360)
    zeroDegLineRenderer = p.line(x=[0, math.cos(top_rad)], y=[0, math.sin(top_rad)], line_color='red', line_width=2, line_dash='dashed')
    zeroDegLineHover = HoverTool(renderers=[zeroDegLineRenderer], tooltips=[('Telescope Azimuth', '0°')])
    p.add_tools(zeroDegLineHover)

    p.axis.visible = False
    p.grid.visible = False

    show(p)

log.info('Plotting function loaded.')

