import os

from dotenv import load_dotenv

load_dotenv()

SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
