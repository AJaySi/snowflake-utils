# Code to get the AWS Secrets.
# Note: expects aws accesskey and secret, locally stored.
# TBD: Parameterize the function to accept SECRET_NAME, REGION_NAME etc.
# Usage : url, user, account, warehouse, password, schema, role, db =
# get_secret()
def get_secret():
  """
  Code to get the AWS Secrets.
  Args:
  None
  Returns:
  Sets/Returns Pass/Fail TC status
  Raises:
  NuTestError
  """
  # TBD: These need to be put somewhere for safety, Or Not.
  # We can create multiple secrets files and pass them here.

  # Create a Secrets Manager client
  session = boto3.session.Session()
  client = session.client(
    service_name='secretsmanager',
    region_name=REGION_NAME
  )

  INFO("Getting snowflake test details from AWS.")
  try:
    get_secret_value_response = \
      client.get_secret_value(SecretId=SECRET_NAME)
  except ClientError as error:
    if error.response['Error']['Code'] == 'DecryptionFailureException':
      # Secrets Manager can't decrypt the protected secret text
      # using the provided KMS key. Deal with the exception here,
      # and/or rethrow at your discretion.
      INFO("Secret Error: {0}".format(error))
      raise error
    elif error.response['Error']['Code'] == 'InternalServiceErrorException':
      # An error occurred on the server side.
      INFO("Error: {0}".format(error))
      raise error
    elif error.response['Error']['Code'] == 'InvalidParameterException':
      # You provided an invalid value for a parameter.
      INFO("Error: {0}".format(error))
      raise error
    elif error.response['Error']['Code'] == 'InvalidRequestException':
      # You provided a parameter value that is not valid for the
      # current state of the resource.
      INFO("Error: {0}".format(error))
      raise error
    elif error.response['Error']['Code'] == 'ResourceNotFoundException':
      # We can't find the resource that you asked for.
      INFO("Error: {0}".format(error))
      raise error
  else:
    # Decrypts secret using the associated KMS CMK.
    # Depending on whether the secret is a string or binary,
    # one of these fields will be populated.
    if 'SecretString' in get_secret_value_response:
      secret = get_secret_value_response['SecretString']
      secret = ast.literal_eval(secret)
      return(
        secret.get("URL"),
        secret.get("User"),
        secret.get("Account"),
        secret.get("Warehouse"),
        secret.get("Password"),
        secret.get("Schema"),
        secret.get("Role"),
        secret.get("Database"))
    else:
      decoded_binary_secret = base64.b64decode(
        get_secret_value_response['SecretBinary'])
      INFO("Binary Encoded: {0}".format(decoded_binary_secret))
      
