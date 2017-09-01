# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 13:14:09 2016

@author: trsenzhang
"""

import logging
import logging.config



# logging
logging.config.fileConfig("logging.conf")
logger = logging.getLogger("report")
'''
logger.debug("debug message")
logger.info("info message")
logger.warn("warn message")
logger.error("error message")
logger.critical("critical message")
logHello = logging.getLogger("hello")
logHello.info("Hello world!")
'''


# sudo pip install pycrypto 
from Crypto.Cipher import AES
from Crypto import Random

BS = 16
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s : s[0:-ord(s[-1])]
key = 'XXXXX'

class AESCipher:
    def __init__(self):
        """
        Requires hex encoded param as a key
        """
        self.key = key.decode("hex") 
        self.cipher = AES.new(self.key,AES.MODE_ECB)

    def encrypt( self, string):
        """
        Returns hex encoded encrypted value!
        """
        encrypted_string = string
        try:
            raw = pad(string) 
            encrypted_string = self.cipher.encrypt(raw).encode('hex')
            encrypted_string = encrypted_string.upper()
        except Exception as e:
            pass        
        return encrypted_string

    def decrypt( self, encrypted_string ):
        """
        Requires hex encoded param to decrypt
        """
        decrypted_string =encrypted_string
        try:
            decrypted_string = unpad(self.cipher.decrypt(encrypted_string.decode('hex')))
        except Exception as e:
            pass        
        return decrypted_string