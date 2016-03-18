copy #table_name# from 's3://#bucket#/#table_name#' 
credentials 'aws_access_key_id=#aws_access_key_id#;aws_secret_access_key=#aws_secret_access_key#;token=#aws_session_token#' 
delimiter ',' region 'us-west-2' REMOVEQUOTES EMPTYASNULL BLANKSASNULL;
