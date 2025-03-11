from utils import get_table_context_json

source_tables = [
    {
        'database': 'DATABASE', 
        'schema': 'SCHEMA', 
        'table': 'USE_CASE_2', 
        'columns': None  # This will fetch all columns in the table
    },
    {
        'database': 'DATABASE', 
        'schema': 'SCHEMA', 
        'table': 'MAPPING_TABLE_1', 
        'columns': None
    },
    {
        'database': 'DATABASE', 
        'schema': 'SCHEMA', 
        'table': 'VW_MAPPING_TABLE_2', 
        'columns': ['COL1', 'COL2', 'COL3'] # Only fetch necessary columns
    },
]

## Table descriptions
descriptions = {
    'USE_CASE_2': """
    ### PLACEHOLDER, HERE COMES ALL THE NECESSARY INFORMATION REGARDING THIS DATASOURCE.
    """,
    'MAPPING_TABLE_1': """
    ### PLACEHOLDER, HERE COMES ALL THE NECESSARY INFORMATION REGARDING THIS DATASOURCE.
    """,
    'VW_MAPPING_TABLE_2': """
    ### PLACEHOLDER, HERE COMES ALL THE NECESSARY INFORMATION REGARDING THIS DATASOURCE.
    """,
}

## Joins to other tables
relationships = { 
    'USE_CASE_2': {
        'COLUMN_1': {
            'reference': 'MAPPING_TABLE_1.COLUMN_1',
            'description': '### PLACEHOLDER - RELATIONSHIP (ONE TO ONE, ONE TO MANY ETC.)'
        },
        'COLUMN_2': {
            'reference': 'VW_MAPPING_TABLE_2.COLUMN_2',
            'description': '### PLACEHOLDER - RELATIONSHIP (ONE TO ONE, ONE TO MANY ETC.)'
        },
        'COLUMN_3': [
            {
                'reference': 'MAPPING_TABLE_1.COLUMN_3',
                'description': '### PLACEHOLDER - RELATIONSHIP (ONE TO ONE, ONE TO MANY ETC.)'
            },
            {
                'reference': 'VW_MAPPING_TABLE_2.COLUMN_3',
                'description': '### PLACEHOLDER - RELATIONSHIP (ONE TO ONE, ONE TO MANY ETC.)'
            }
        ],
    }
}

## Example queries to increase accuracy
examples = [
    {
        'user_input': '### PLACEHOLDER - EXAMPLE QUESTION',
        'sql_query': """
        SELECT ... ### EXAMPLE QUERY
        FROM DATABASE.SCHEMA.TABLE
        WHERE COLUMN1 IN ('X', 'Y')
        """
    },###...
]

# Combine the general prompt and table details to generate the system prompt 
table_context = get_table_context_json(source_tables, descriptions, relationships, examples)
