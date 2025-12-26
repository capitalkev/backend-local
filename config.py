"""
Configuración y constantes para el servicio de Gmail
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Gmail API Configuration
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
USER_ID = "me"
TOKEN_FILE = "token.json"

# Environment Variables
MI_EMAIL = os.getenv("MI_EMAIL")

# Server Configuration
HOST = "localhost"
PORT = 8001
