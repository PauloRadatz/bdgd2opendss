import os
import pathlib

script_path = os.path.dirname(os.path.abspath(__file__))

bdgd2dss_private_json = pathlib.Path(script_path).joinpath("bdgd2dss_private.json")
bdgd2dss_json = pathlib.Path(script_path).joinpath("bdgd2dss.json")


#TODO Add other here
