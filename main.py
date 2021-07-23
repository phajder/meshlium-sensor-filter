from pathlib import Path

import json
import shutil
import mysql.connector as conn
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def get_sensors_on_wasps(db, date_start, date_end):
    """
    TODO: docs
    """
    c = db.cursor()
    c.execute(f"SELECT DISTINCT id_wasp, sensor FROM sensorParser WHERE timestamp BETWEEN \'{date_start}\' AND \'{date_end}\'")
    sensors_on_wasps = c.fetchall()
    c.close()
    return sensors_on_wasps


def get_data_by_interval(db, id_wasp, sensor, date_start, date_end):
    """
    TODO: docs
    """
    c= db.cursor()
    query = f"SELECT value, timestamp FROM sensorParser WHERE id_wasp = \'{id_wasp}\' AND sensor = \'{sensor}\' AND timestamp BETWEEN \'{date_start}\' AND \'{date_end}\'"
    c.execute(query)
    rs = c.fetchall()
    c.close()
    return rs

if __name__ == "__main__":
    # Read all configuration parameters
    config = json.loads(open('config.json').read())
    db_config = config['db']
    date_start = config['dateStart']
    date_end = config['dateEnd']
    skip_sensors = config['skipSensors']

    # Create ./data folder if not exists
    data_path = Path('./data')
    if data_path.exists() and data_path.is_dir():
        shutil.rmtree(data_path)
    data_path.mkdir(parents=True, exist_ok=True)

    # Get database connection
    db = conn.connect(
        host=db_config['dbHost'],
        port=db_config['dbPort'],
        user=db_config['dbUser'],
        password=db_config['dbPass'],
        database=db_config['database']
    )
    # Get all sensors to check
    sensors_on_wasps = get_sensors_on_wasps(db, date_start, date_end)
    # Query all sensors on every WASP station
    for wasp in sensors_on_wasps:
        wasp_id = wasp[0]
        sensor_name = wasp[1]
        # Skip all sensors, which does not have numeric value (not-plottable)
        if sensor_name in skip_sensors:
            continue
        data = get_data_by_interval(db, wasp_id, sensor_name, date_start, date_end)
        x = [ d[1] for d in data ] # timestamp
        y = [ float(d[0]) for d in data ] # sensor value

        # Formatting plot properly
        fig, ax = plt.subplots()
        fig.set_size_inches(18.5, 10.5)
        ax.set_title(f"{wasp_id}-{sensor_name}")
        ax.set_xlabel('Timestamp')
        ax.set_ylabel('Sensor param value')
        ax.plot_date(x, y, 'k-')
        hfmt = mdates.DateFormatter('%y-%m-%d %H:%M')
        ax.xaxis.set_major_formatter(hfmt)
        plt.gcf().autofmt_xdate()
        # plt.plot(x, y)

        # Saves figure in ./data folder as wasp_id-sensor_name.png
        path = data_path.joinpath(wasp_id)
        path.mkdir(parents=True, exist_ok=True)
        plt.savefig(path.joinpath(f"{sensor_name}.png").as_posix())
        plt.close()