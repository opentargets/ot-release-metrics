# Metris visualisation and comparison using a Streamlit app
The app is deployed here: **https://open-targets-metrics.streamlit.app/**


## Running the web app to visualise and compare the metrics
To start the app locally, run `streamlit run app.py`.

If you encounter a `TomlDecodeError`, this can be resolved by removing the `~/.streamlit` folder.


## Deploying the app
The application will be **automatically deployed** each time the `master` branch is updated.

The hosting is served using Streamlit's native Cloud service.


## Rebooting the app
When there are any changes to either the code or data (release metrics) used by the app, it will need to be rebooted. In order to do that:
In order to do that:
1. Log in to https://share.streamlit.io/ with your work GitHub account.
2. In the upper right corner, click on your username and switch to the _opentargets_ workspace.
3. Next to the _ot-release-metrics_ app, click on a three dot icon and then choose “Reboot”.

> **Note**
> A “Rerun” option, available inside the app itself from a sandwich dropdown menu in the upper right corner, is _different_ to the “Reboot” option described above, and will not work to make the app ingest the new data.

## Deleting a metrics run file from the app
If a metrics run file is no longer needed, it can be deleted from the app. In order to do that:
1. Go to the Explore tab and select the file to be deleted.
2. Click on the "Delete release metrics" button.
3. Confirm the deletion by providing the password in the pop-up window. This password can be found in the app secrets in Streamlit Cloud or in the or in the Open Targets central login spreadsheet.

!!! warning
    This will delete data stored in Google Cloud Storage, and it cannot be undone.