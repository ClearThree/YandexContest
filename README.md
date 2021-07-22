# YandexContest for Backend school 2021.

REST app for candy delivery service.

App uses FastAPI, Uvicorn, Pydantic and Pytest libraries (reflected in requirements).

# Install and run the app

0. If not installed, install git and virtualenv with commands:
   ```sudo apt install python3-venv``` and
   ```sudo apt install git```
1. Clone the repository
2. Create the virtual environment for project:
   ```python3 -m venv delivenv```
3. Activate the environment:
   ```source delivenv/bin/activate```
4. Get into the directory of repo and install required libraries with
   ```pip3 install -r requirements.txt```
5. Run ```uvicorn main:app --host 0.0.0.0 --port 8000```

# Run tests

6. Perform steps 0-4 if not performed yet.
7. Run ```pytest```

# Setting the autostart after reboot

To deploy the app as system service follow these steps:

1. Get into repo directory
2. Run ```sudo cat service.txt >delivery.service```
3. Do ```sudo mv delivery.service /etc/systemd/system/```
4. Enable the service with ```sudo systemctl enable delivery.service```
5. Finally, start the service with ```sudo systemctl start delivery.service```

Note: If the virtualenv you created has the name that differs from "delivenv", then open delivery.service file and
change the name to yours in 'Environment=...' and 'ExecStart=' paths.
