

import pandas as pd
import base64
import io
input_string = 'UHJvamVjdCBDb2RlLHN0YXJ0X2RhdGUscmVxdWVzdG9yLGRlc2NyaXB0aW9uLGVtYWlsLHNvdXJjZXMsZXZlbnRfYXR0cmlidXRlLGRhdGFfdHlwZSxvcGVyYXRvcix2YWx1ZXMNCkFEMDAxLDUvMjAvMjAxOSxKYXNhLGtleXdvcmRtYXRjaCxqYXNhLmNob3VkaGFyeUBiY29uZS5jb20sImh0dHBzOi8vZWRpdGlvbi5jbm4uY29tLywgaHR0cHM6Ly93d3cudGhlaGluZHUuY29tLyIsU3VtbWFyeSxzdHJpbmcsY29udGFpbnMsImNvcHBlciwgIHRpbiwgbGVhZCINClREMDAxLDUvMjAvMjAxOSxUaHVsYXNpLGtleXdvcmRtYXRjaCx0aHVsYXNpcmFtLmtAYmNvbmUuY29tLCJodHRwczovL2VkaXRpb24uY25uLmNvbS8sIGh0dHBzOi8vd3d3LnRoZWhpbmR1LmNvbS8iLEhlYWRsaW5lLHN0cmluZyxjb250YWlucyxsZWFkDQo='
"""
{
  "data": [
    {
      "project_code": "a",
      "start_date": "2019-03-20T18:30:00.000Z",
      "requestor": "sas",
      "description": "sasasa",
      "email_id": [
        "jasaa@bc"
      ],
      "sources": "SENSE-253",
      "event_attributes": [
        "Tags"
      ],
      "expression_type": "String",
      "comparator": "greater_than",
      "values": "12113"
    }
  ]
}


"""


decoded_string = base64.b64decode(input_string)
io_string = io.BytesIO()
io_string.write(decoded_string)
io_string.seek(0)
dataframe = pd.read_csv(io_string)
print(dataframe)

for i in range(len(dataframe)):
    row = dataframe.iloc[i]
    dictt = {}
    print(type(row['start_date']))
    dictt.update({"project_code": row["Project Code"], "start_date": row['start_date'],
                "requestor": row['requestor'], "description": row["description"],
                   "email_id": [row['email']],
                   "sources": [ii.strip() for ii in row['sources'].split(",")],
                   "event_attributes": [ea.strip() for ea in row['event_attribute'].split(",")],
                   "expression_type": str(row['data_type']),
                   "comparator": str(row['operator']),
                   "values": [v.strip() for v in row['values'].split(",")]

                   })
    input = {"data": [dictt]}
    
    #df_to_json = {}