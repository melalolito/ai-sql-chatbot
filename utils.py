import string, secrets, time, re, json, os
import streamlit as st
import pandas as pd
from datetime import datetime, date
from snowflake.connector import connection
from snowflake.connector.pandas_tools import write_pandas
from config_oidc import sf_conn

# Fun spinner messages
spinner_messages = [
    "Let me fetch that information for you...",
    "On it! Gathering your data now...",
    "Your data is coming right up...",
    "Hold tight, crunching data with gusto...",
    "Abracadabra! Summoning your data...",
    "Data elves are working hard, please wait..."
]

# Get metadata from Snowflake and merge with the descriptions above
@st.cache_data(hash_funcs={connection.SnowflakeConnection: lambda _: None}, show_spinner='Waking AI up...')
def get_table_context_json(table_dict, descriptions, groupings, relationships, examples):
    """
    Fetch metadata for tables from Snowflake and return it as a JSON object.
    The JSON includes table descriptions, column metadata, default column groupings, joins, and examples.

    Args:
        table_dict (list of dict): A list of dictionaries, each containing metadata for a table.
            Each dictionary should have the following keys:
                - 'table' (str): The name of the table.
                - 'schema' (str): The schema containing the table.
                - 'database' (str): The database containing the schema.
                - 'columns' (list of str, optional): Specific columns to include. If not provided, all columns are included.
        
        descriptions (dict): A dictionary containing table descriptions. Keys are table names, and values are descriptions (str).
        
        groupings (dict): A dictionary defining default column groupings. Keys are table names, and values are dictionaries with column names as keys and grouping details as values.
        
        relationships (dict): A dictionary defining join relationships for columns. Keys are table names, and values are dictionaries with column names as keys and join information as values.
        
        examples (list of dict): A list of example queries or usage examples to include in the JSON output.

    Returns:
        str: A formatted JSON string containing the table metadata.
    """

    table_json = {'tables': [], 'examples':[]}

    for example in examples:
        table_json['examples'].append(example)

    for table_info in table_dict:
        table = table_info['table']
        schema = table_info['schema']
        database = table_info['database']
        required_columns = table_info.get('columns') ## Only fetch columns that we define as necessary to decrease prompt token usage

        if required_columns:
            column_filter = 'AND COLUMN_NAME IN ({})'.format(', '.join([f"'{col}'" for col in required_columns]))
        else:
            column_filter = ''

        # Fetch columns DDL from Snowflake
        cursor = sf_conn.cursor()

        columns = cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, COMMENT
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        {column_filter}
        """).fetch_pandas_all()

        cursor.close()

        columns_list = []
        
        for i in range(len(columns['COLUMN_NAME'])):
            column_name = columns['COLUMN_NAME'][i]
            column_data_type = columns['DATA_TYPE'][i]
            column_description = columns['COMMENT'][i] if columns['COMMENT'][i] else 'No description available'
            column_grouping_info = groupings.get(table, {}).get(column_name, {})
            column_joins = relationships.get(table, {}).get(column_name, {})
            
            columns_list.append({
                'column_name': column_name,
                'column_type': column_data_type,
                'column_description': column_description,
                'column_joins': column_joins
            })
        
        table_description = descriptions.get(table, {})

        table_json['tables'].append({
            'name': table,
            'schema': schema,
            'database': database,
            'description': table_description,
            'columns': columns_list,
        })

    return json.dumps(table_json, indent=4)

def generate_question_id(length=16):
    """
    Generate a random alphanumeric identifier for each question.

    Args:
        length (int, optional): The length of the identifier. Default is 16.

    Returns:
        str: A randomly generated alphanumeric string of the specified length.
    """

    alphabet = string.ascii_letters + string.digits
    key = ''.join(secrets.choice(alphabet) for _ in range(length))
    return key     

def execute_sql(sql):
    """
    Execute a SQL query and return a DataFrame if successful. Returns an error message otherwise.

    Args:
        sql_match (re.Match): A regular expression match object containing the SQL query in the first capturing group.

    Returns:
        pd.DataFrame: A DataFrame containing the results of the executed SQL query if successful.
        str: An error message if the query execution fails or if the query contains potentially dangerous DML statements.
    """

    # Check for DML statements
    if re.search(r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b', sql, re.IGNORECASE):
        return 'Nice try, but dangerous changes to our data is not allowed here! Try asking question about our data instead.'

    cursor = sf_conn.cursor()

    try:
        result = cursor.execute(sql).fetch_pandas_all()
        return result.dropna(how='all')
    
    except Exception as e:
        error_message = str(e)
        match = re.search(r'SQL compilation error:', error_message)

        if match:
            return error_message.split('SQL compilation error:', 1)[1]
        else:
            return error_message
    finally:
        cursor.close()

def plot_dataframe(question_id, df):
    """Plot numeric columns from the dataframe with an intelligent chart type selector and customizable axes."""
    
    if df.empty:
        st.warning("No data available for plotting.")
        return

    # Rename columns for readability
    df = df.rename(columns={col: col.replace("_", " ").title() for col in df.columns})

    # Detect possible X-axis columns (preferably time series or categorical)
    x_options = df.select_dtypes(include=['datetime64[ns]', 'object']).columns.tolist()

    if not x_options: # If no clear categorical/time-based X, use the first column by default
        x_options = [df.columns[0]]

    default_x = x_options[0]
    
    # Convert X column to datetime if possible
    is_time_series = False
    if pd.api.types.is_datetime64_any_dtype(df[default_x]):
        is_time_series = True
    else:
        try:
            df[default_x] = pd.to_datetime(df[default_x])
            is_time_series = True
        except Exception:
            pass

    # Detect possible Y-axis columns (numeric only)
    y_options = df.select_dtypes(include=['number']).columns.tolist()

    if not y_options:
        st.error("No numeric columns available for plotting.")
        return

    with st.expander('Edit chart'):
        # Default chart selection
        default_chart = '📈 Line chart' if is_time_series else '📊 Bar chart'
        chart_options = ("📈 Line chart", "📊 Bar chart")
        chart_choice = st.selectbox("Select chart type", chart_options, index=chart_options.index(default_chart), key=f'chart_{question_id}')

        # Let users choose X and Y axes
        axis_row = st.columns([1,1])
        x_axis = axis_row[0].selectbox("Select X-axis", x_options, index=df.columns.get_loc(default_x))
        y_axis = axis_row[1].selectbox("Select Y-axis", y_options, index=0)
        #y_axis = axis_row[1].multiselect("Select Y-axis", y_options, default=[y_options[0]])

    # Plot the selected chart
    st.write(f'**{y_axis} by {x_axis}**')

    if chart_choice == "📈 Line chart":
        st.line_chart(df, x=x_axis, y=y_axis)
    elif chart_choice == "📊 Bar chart":
        st.bar_chart(df, x=x_axis, y=y_axis)

def write_data_to_sf(df, database_name, schema_name, table_name):
    """
    Writes a DataFrame to a specified table in the Snowflake.

    Args:
        df (pd.DataFrame): The DataFrame to be written. The DataFrame should have columns matching the structure of the target table in Snowflake.
        table_name (str): The name of the target table in Snowflake.

    Returns:
        None
    """
    try:
        write_pandas(sf_conn, df, database=database_name, schema=schema_name, table_name=table_name)
        return 'Success'
    except Exception as e:
        return e


def update_feedback(feedback_score, feedback_text, question_id):
    """
    Update chat logs with provided user feedback.

    Args:
        feedback_score (int or float): The score representing the user's feedback.  Should be a numeric value, typically a rating score (e.g., 1-5).
        feedback_text (str): The text feedback provided by the user. This can be a string describing the user's experience or suggestions.
        question_id (str): The unique identifier of the question being reviewed. This is used to match the specific record in the `CHAT_HISTORY` table.

    Returns:
        None
    """

    update_table_query = """
    UPDATE DATABASE.SCHEMA.TABLE 
    SET FEEDBACK_SCORE = %s, FEEDBACK_TEXT = %s
    WHERE QUESTION_ID = %s
    """
    cursor = sf_conn.cursor()

    try:
        cursor.execute(update_table_query, (feedback_score, feedback_text, question_id))
        feedback_container = st.empty()
        feedback_container.success('Thank you for your feedback!')
        time.sleep(2)
        feedback_container.empty()
    except Exception as e:
        st.error(f'An error occurred while logging your feedback: {e}.')
    finally:
        cursor.close()

# Popover dialog
@st.dialog('🐞 Report a Bug')
def report_bug():
    """
    Allows users to report bugs and submit feedback through a form. The user provides their email, a description of the bug, and optionally steps to reproduce the issue. 
    The data is then submitted to a Snowflake database for logging and follow-up. 
    """

    st.caption("""
    This form is for reporting app performance issues—things like crashes or glitches.

    If you’re experiencing incorrect answers or missing data, please:
    - Use the smiley feedback widget below each answer to let us know.
    - Or hop over to [#help-slack](PLACEHOLDER_SLACK_LINK) on Slack—we're happy to help!""")

    reporter_email = st.text_input('Your email address [Required]:', placeholder='Please enter your email so we can follow up if needed.')
    bug_description = st.text_area('Describe the issue [Required]:', placeholder="E.g., 'The app crashes when I click on the Submit button.'")
    reproduction_steps = st.text_area('Steps to reproduce the issue [Optional]:', placeholder="E.g., '1. Go to the homepage\n2. Click on the Submit button\n3. Observe the error message'")

    if st.button('Submit', key='submit_bug', disabled=(reporter_email=='' or bug_description=='')):

        if bug_description and reporter_email and re.match(r'^[\w\.-]+@COMPANY_NAME\.\w+$', reporter_email) is not None:

            st.session_state.bug_report = {
                'reporter_email': reporter_email,
                'description': bug_description,
                'reproduction_steps': reproduction_steps,
                'reported_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
        
            bug_df = pd.DataFrame([{
                        'REPORTER_EMAIL': st.session_state.bug_report['reporter_email'],
                        'DESCRIPTION': st.session_state.bug_report['description'],
                        'REPRODUCTION_STEPS': st.session_state.bug_report['reproduction_steps'],
                        'REPORTED_ON': st.session_state.bug_report['reported_on']
                        }])

            with st.spinner('Submitting your bug report...'):
                bug_submission_status = write_data_to_sf(bug_df, 'BUG_REPORTS') 

            if bug_submission_status == 'Success':
                st.success("Thank you for reporting the bug! We'll look into it.")
                time.sleep(2)
                st.rerun()
            else:
                st.error(f'An error occurred while saving your bug: {bug_submission_status}. Please try again or reach out to [Melisa Kocbas](https://slack.com/app_redirect?channel=U03J7NV0XQF) if the issue persists.')
        
        elif re.match(r'^[\w\.-]+@tripadvisor\.\w+$', reporter_email) is None:
            st.error('Please enter a valid email address.')
        
        else:
            st.error('Please fill out all required fields before submitting.')
