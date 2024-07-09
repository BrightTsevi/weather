import requests
import pytz
from datetime import timedelta, datetime
import boto3
import json
import os
from dotenv import load_dotenv
DEFAULT_LATITUDE = '36.8247182'
DEFAULT_LONGITUDE = '-76.2529502'

load_dotenv()


class ExtractData:
    def __init__(self, longitude = None, latitude = None):
        if not longitude or not latitude:
            self.longitude = DEFAULT_LONGITUDE
            self.latitude = DEFAULT_LATITUDE
        key = os.environ.get('weather_api_key')
        print(key)
        self.weather_endpoint = f"https://api.openweathermap.org/data/3.0/onecall?lat={self.longitude}&lon={self.latitude}&units=metric&appid={key}"

        self.s3 = boto3.resource('s3')

    def save_json_to_s3(self, json_string, destination) -> None:

        s3object = self.s3.Object('bright-pipelines-warehouse', destination)
        s3object.put(
            Body=(bytes(json.dumps(json_string).encode('UTF-8')))
        )

    def get_data(self) -> json:
        return requests.get(self.weather_endpoint).json()

    @staticmethod
    def temp_to_fahrenheit(temp_in_kelvin) -> float:
        temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9 / 5) + 32
        return temp_in_fahrenheit

    @staticmethod
    def timestamp_to_est(time, timezone_offset):
        utc = datetime.utcfromtimestamp(time + timezone_offset)
        utc = utc.astimezone(pytz.utc).isoformat()
        return utc

    @property
    def fetch_transforom_data(self):
        response = self.get_data()

        print(response)
        timezone_offset = response["timezone_offset"]
        response = response["current"]
        out_json = {
             'dt': response["dt"],
             'utc_time': self.timestamp_to_est(response["dt"], timezone_offset),
             # 'sunrise': response["sunrise"],
             # 'sunset': response["sunset"],
             'temp': response["temp"],
             'temp_fahrenheit': self.temp_to_fahrenheit(response["temp"]),
             'feels_like': response["feels_like"],
             'pressure': response["pressure"],
             'humidity': response["humidity"],
             'dew_point': response["dew_point"],
             'uvi': response["uvi"],
             'clouds': response["clouds"],
             'visibility': response["visibility"],
             'wind_speed': response["wind_speed"],
             'wind_deg': response["wind_deg"],
             'weather': response["weather"][0]["main"],
             'weather_description': response["weather"][0]["description"]
        }

        filename = f"test/weather_{response['dt']}"
        self.save_json_to_s3(out_json, filename)
        return out_json


if __name__ == "__main__":
    etl = ExtractData()
    out = etl.fetch_transforom_data
    print(out)