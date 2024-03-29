from app.main import create_app
import os
print (os.getcwd())
app = create_app(os.environ.get('BOILERPLATE_ENV') or 'dev')
app.run(host="127.0.0.1", port=8081, debug=True, use_reloader=False)