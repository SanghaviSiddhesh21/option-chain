## once the parent directory to this folder is selected run this file
virtualenv env
source env/bin/activate
pip install flask
pip install sqlalchemy
pip install flask_sqlalchemy
pip install flask_restful
pip install flask_cors
pip install celery
pip install celery[redis]
pip install numpy
pip install scipy
## Once this file is executed again run source env/bin/activate in the terminal and then the required file