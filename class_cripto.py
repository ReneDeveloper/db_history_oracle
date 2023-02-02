"""class_cripto.py"""

from cryptography.fernet import Fernet
from class_config import Config

cfg = Config()

# we will be encrypting the below string.
message = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
message = 'oracle+cx_oracle://user:pw@server:port/instance'

#key = Fernet.generate_key()
key = cfg.get_par('crkey')
#print(f'key:{key}')

# Instance the Fernet class with the key
fernet = Fernet(key)

# then use the Fernet class instance
# to encrypt the string string must
# be encoded to byte string before encryption
encMessage = fernet.encrypt(message.encode())

print("original  string: ", message)
print("encrypted string: ", encMessage)

# decrypt the encrypted string with the
# Fernet instance of the key,
# that was used for encrypting the string
# encoded byte string is returned by decrypt method,
# so decode it to string with decode methods
decMessage = fernet.decrypt(encMessage).decode()

print("decrypted string: ", decMessage)
