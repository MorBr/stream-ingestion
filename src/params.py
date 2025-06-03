import os

# Define a helper function to get config, prioritizing environment variables
def get_config_value(env_var_name, default=None):
  """Gets config from environment variable or returns default."""
  value = os.getenv(env_var_name)
  if value:
    print(f"Using value from environment variable: {env_var_name}")
    return value
  # Optional: Add fallback to properties file or raise an error
  # elif default:
  #   print(f"Using default value for {env_var_name}")
  #   return default
  else:
    # Decide: return None, return default, or raise error if required
    # raise ValueError(f"Required configuration '{env_var_name}' not found in environment variables.")
    print(f"Warning: Environment variable '{env_var_name}' not set. Using default: {default}")
    return default # Or None, or raise error

# --- Use the function ---
# Match the env_var_name to the GitHub Secret names you created
sf_user = get_config_value('SNOWFLAKE_USER')
sf_password = get_config_value('SNOWFLAKE_PASSWORD')
sf_account = get_config_value('SNOWFLAKE_ACCOUNT')
sf_db = get_config_value('SNOWFLAKE_DATABASE')
sf_schema = get_config_value('SNOWFLAKE_SCHEMA')
sf_warehouse = get_config_value('SNOWFLAKE_WAREHOUSE')
sf_url = get_config_value('SNOWFLAKE_URL')
kafka_servers = get_config_value('KAFKA_BOOTSTRAP_SERVERS')


# --- Now use these variables in your connection logic ---
# Example:
if not sf_user or not sf_password or not sf_account:
  raise ValueError("Missing required Snowflake credentials!")

print(f"Connecting to Snowflake account: {sf_account} as user: {sf_user}")
# ... rest of your Snowflake connection logic using sf_user, sf_password, sf_account

if not kafka_servers:
    raise ValueError("Missing required Kafka bootstrap servers!")

print(f"Configuring Kafka with servers: {kafka_servers}")
# ... rest of your Kafka configuration logic using kafka_servers, kafka_sasl_config