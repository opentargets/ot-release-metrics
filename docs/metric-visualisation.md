# Metris visualisation and comparison using a Streamlit app

The app is deployed here: **https://share.streamlit.io/opentargets/ot-release-metrics/app.py**

## Running the web app to visualise and compare the metrics
To start the app locally, run `streamlit run app.py`.

If you encounter a `TomlDecodeError`, this can be resolved by removing the `~/.streamlit` folder.

## Update the web app
The application will be **automatically deployed** each time the `master` branch is updated.

The hosting is served using Streamlit's native Cloud service.

To make any changes, simply sign up at https://share.streamlit.io/signup with your personal GitHub account making sure access to the opentargets organisation repos is granted. You should be able to see and change the `ot-release-metrics` app configuration.
