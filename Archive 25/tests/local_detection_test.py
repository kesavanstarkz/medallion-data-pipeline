from services.fabric.deploy_service import FabricDeployService

# Create a fake Copy activity with DelimitedText sink
original_activity = {
    "name": "CopyActivity1",
    "type": "Copy",
    "typeProperties": {
        "source": {
            "type": "DelimitedTextSource",
            "datasetSettings": {
                "type": "DelimitedText",
                "typeProperties": {"columnDelimiter": ","},
                "externalReferences": {"connection": "11111111-1111-1111-1111-111111111111"}
            }
        },
        "sink": {
            "type": "DelimitedTextSink",
            "datasetSettings": {
                "type": "DelimitedText",
                "typeProperties": {"columnDelimiter": ","},
                "externalReferences": {"connection": "22222222-2222-2222-2222-222222222222"}
            }
        }
    },
    "inputs": [{"referenceName": "ds_old_src"}],
    "outputs": [{"referenceName": "ds_old_snk"}]
}

service = FabricDeployService(access_token="fake")

# Detect sink type
detected_sink = service._detect_connector_type(original_activity, "sink")
print("Detected sink type:", detected_sink)

# Simulate selected type mismatch
selected = "REST"
print("Selected sink type:", selected)
if selected.upper() != detected_sink.upper():
    print(f"Validation: Selected sink type {selected} does not match actual pipeline sink type {detected_sink}")
else:
    print("Validation: Selected matches detected")

# Simulate selected matches
selected2 = "DelimitedText"
print("Selected sink type:", selected2)
if selected2.upper() != detected_sink.upper():
    print(f"Validation: Selected sink type {selected2} does not match actual pipeline sink type {detected_sink}")
else:
    print("Validation: Selected matches detected")

# Show extracted connection id from endpoint structure via low-level logic
conn = ((original_activity.get('typeProperties') or {}).get('sink') or {}).get('datasetSettings', {}).get('externalReferences', {}).get('connection')
print('Extracted sink connection id:', conn)
