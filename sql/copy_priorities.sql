copy d_priorities from 's3://#bucket#/PrioritySLA.csv' 
credentials 'aws_access_key_id=#aws_access_key_id#;aws_secret_access_key=#aws_secret_access_key#;token=#aws_session_token#'
delimiter ',' region 'us-west-2' REMOVEQUOTES IGNOREHEADER as 1;
