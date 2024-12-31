from flask import Flask, jsonify
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialize Flask app
app = Flask(__name__)

# Initialize a session using Amazon S3
s3 = boto3.client('s3')
BUCKET_NAME = 'sachin-bucket-assignment' 

# Function to list the content of the S3 bucket
def list_s3_objects(path=""):
    try:
        # List objects in the S3 bucket under the specified path
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=path, Delimiter='/')
        
        directories = []
        files = []

        if 'CommonPrefixes' in response:
            directories = [prefix['Prefix'].split('/')[-2] for prefix in response['CommonPrefixes']]
        
        if 'Contents' in response:
            files = [content['Key'].split('/')[-1] for content in response['Contents']]

        content = directories + files

        return jsonify({"content": content})

    except (NoCredentialsError, PartialCredentialsError) as e:
        return jsonify({"error": "AWS credentials are not configured correctly."}), 403
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Create a route for the Flask app
@app.route('/list-bucket-content/', defaults={'path': ''}, methods=['GET'])
@app.route('/list-bucket-content/<path:path>', methods=['GET'])
def list_bucket_content(path):
    return list_s3_objects(path)

# Run the Flask app on localhost
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
