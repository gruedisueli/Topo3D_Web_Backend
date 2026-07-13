import os
import hashlib

pepper = os.getenv("IP_HASH_PEPPER") #used for generating unique hashes for anonymized IP address storage
if pepper is None: 
    print("Ip hash pepper env variable is not set")
    exit(1)
    
def hash_ip(ip: str) -> str:
    return hashlib.sha256((pepper + ip).encode()).hexdigest()