import json
import os
from datetime import date




def SendLocal(load, name_file,path):
    with open(f"{path}{name_file}.json", "w") as file:
        json.dump(load,file)
        


    
    