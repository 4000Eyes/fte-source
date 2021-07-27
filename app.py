from app.app import create_app

import os
app = create_app(os.getenv('BOILERPLATE_ENV') or 'dev')


