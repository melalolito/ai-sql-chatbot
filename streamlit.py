import streamlit as st
from openai import OpenAI
from streamlit_feedback import streamlit_feedback
from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx
import re, time, pytz, os, random
import pandas as pd
from datetime import date, datetime

st.set_page_config(page_title='AI SQL Chatbot', 
                   initial_sidebar_state='expanded',
                   page_icon='logos/logo.png')

from prompts.main import use_cases, generate_prompt
from utils import spinner_messages, generate_question_id, execute_sql, plot_dataframe, report_bug, write_data_to_sf, update_feedback

# Inject CSS
with open('styles.css') as style:
    st.html(f'<style>{style.read()}</style>')

# OpenAI credentials
client = OpenAI(
    organization = os.environ.get('OPENAI_ORGANIZATION'),
    api_key = os.environ.get('OPENAI_API_KEY'))

# Initialize session state
for key in ['session_id', 'use_case', 'feedback']:
    if key not in st.session_state:
        st.session_state[key] = None

for key in ['messages', 'chat_history']:
    if key not in st.session_state:
        st.session_state[key] = []

# Functions to control immediate effect on session state
def select_question():
    st.session_state.selected_question = True

def set_initial_use_case():
    st.session_state.use_case = st.session_state.initial_use_case
    st.session_state.messages = [{'role': 'developer', 'content': generate_prompt(st.session_state.use_case)}]

def change_use_case():
    st.session_state.use_case = st.session_state.sidebar_use_case
    st.session_state.messages = [{'role': 'developer', 'content': generate_prompt(st.session_state.use_case)}]

def clear_chat():
    st.session_state.chat_history = []
    st.session_state.messages = [{'role': 'developer', 'content': generate_prompt(st.session_state.use_case)}]

# List of available use cases
use_case_names = [use_case[0] for use_case in use_cases.items()]

# Generate session ID 
ctx = get_script_run_ctx()
st.session_state.session_id = ctx.session_id

with st.sidebar:
    ta_logo = st.logo("logos/ta_logo.png", size="large")

    if st.session_state.use_case is not None:

        st.header("Current use case")
        st.selectbox(
            "Active use case",
            label_visibility="collapsed",
            key="sidebar_use_case",
            options=use_case_names,
            index=use_case_names.index(st.session_state.use_case),
            help="Your session will restart once you switch the active use case.",
            on_change=change_use_case,
        )

        st.write('')

        cta1, cta2 = st.columns([1,1])
        cta1.button("🐞 Report a bug", on_click=report_bug, use_container_width=True)
        cta2.button("🗑️ Clear chat", on_click=clear_chat, use_container_width=True)

        st.divider()

    st.divider()

    st.header("Useful links")
    st.write("💬 [Slack channel](PLACEHOLDER_LINK_SLACK)")
    st.write("📚 [Documentation](PLACEHOLDER_LINK_USER_GUIDE)")

# Landing page - use case selector without chat input
if st.session_state.use_case is None:

    st.header('Hi! What do you want to explore today?')
    use_case_dropdown = st.selectbox(
        'Your use case', 
        label_visibility='collapsed',
        key='initial_use_case', 
        options=use_case_names, 
        index=None,
        placeholder='Choose your use case to wake chatbot up.',
        on_change=set_initial_use_case)

# Enable chat input only after any use case is selected
else:

    # Display existing chat messages
    for message in st.session_state.messages:
        if message['role'] == 'developer': # Hide the system prompt from chat UI
            continue

        if message['role'] == 'assistant':
            with st.chat_message(message['role'], avatar='logos/trippy_logo.png'):
                st.markdown(message['content'])
                
                # Check if message contains a dataframe or if SQL returned an error
                if isinstance(message.get('sql_result'), pd.DataFrame):
                    st.dataframe(message['sql_result'], hide_index=True)
                elif message.get('error') is not None:
                    st.error(message['error'])

        else:
            with st.chat_message(message['role']):
                st.markdown(message['content'])

    # Initialize prompt variable
    prompt = None

    # Chat widget to receive user input
    if user_input := st.chat_input('Ask me anything...', on_submit=select_question):
        prompt = user_input

    st.markdown("""<div class="warning-footer">❗AI can make mistakes. Consider validating important information with analytics.</div>""", unsafe_allow_html=True)

    if prompt:
        st.session_state.messages.append({'role': 'user', 'content': prompt})
        with st.chat_message('user'):
            st.markdown(prompt)

    # If last message is not from assistant, generate a new response
    if st.session_state.messages[-1]['role'] != 'assistant':

        question_id = generate_question_id()

        ai_start_time = time.time()

        with st.chat_message('assistant', avatar='logos/logo.png'):
            
            message_placeholder = st.empty()
            response = ''

            stream = client.chat.completions.create(
                model='chatgpt-4o-latest', 
                temperature=0,
                messages=[
                    {'role': m['role'], 'content': m['content']}
                    for m in st.session_state.messages
                ],
                stream=True,
                stream_options={'include_usage': True} # st.write_stream doesn't seem to work with include_usage for now, opting to manual stream below
            )
            #response = st.write_stream(stream)

            for chunk in stream:
                if chunk.choices:
                    response += chunk.choices[0].delta.content or ''
                    message_placeholder.markdown(response + '▕') # Stream chunks with typewriter effect
                
                if chunk.usage:
                    completion_tokens = chunk.usage.completion_tokens
                    prompt_tokens = chunk.usage.prompt_tokens

            message_placeholder.markdown(response) # Show full response

            ai_response_time = round(time.time() - ai_start_time, 2) # Time to generate the AI response

            # Parse the response for a SQL query and execute
            sql_match = re.search(r'`sql\n(.*)\n`', response, re.DOTALL)

            if sql_match:

                query_start_time = time.time()

                with st.spinner(random.choice(spinner_messages)):
                    result = execute_sql(sql_match.group(1))

                query_time = round(time.time() - query_start_time, 2) # Time to run the SQL query
                
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, hide_index=True)
                    query_result = result
                    error_message = None
                else: 
                    st.error(result)
                    query_result = None
                    error_message = result

            st.session_state.messages.append({'role': 'assistant',
                                            'question_id': question_id,
                                            'content': response,
                                            'query': sql_match.group(1) if sql_match else None,
                                            'sql_result': query_result if sql_match else None,
                                            'sql_error': error_message if sql_match else None,
                                            'prompt_tokens': prompt_tokens,
                                            'completion_tokens': completion_tokens,
                                            'ai_response_time': ai_response_time,
                                            'query_time': query_time if sql_match else None,
                                            'use_case': st.session_state.use_case})
            
        # Show the feedback widget and generate chat logs (only for messages after the initial system prompt)
        if(len(st.session_state.messages) > 2):

            streamlit_feedback(
                feedback_type = 'faces',
                align = 'flex-start',
                optional_text_label = '[Optional] Please provide more feedback.',
                key = 'feedback')
            
            chat_history_entry = {
                'QUESTION_ID': st.session_state.messages[-1]['question_id'],
                'DS': date.today(),
                'TIMESTAMP': datetime.now(pytz.timezone('Europe/Lisbon')).strftime('%Y-%m-%d %H:%M:%S'),
                'SESSION_ID': st.session_state.session_id,
                'QUESTION': prompt,
                'FULL_ANSWER': st.session_state.messages[-1]['content'],
                'SQL_QUERY': st.session_state.messages[-1]['query'],
                'QUERY_RESULT': st.session_state.messages[-1]['sql_result'].to_json(orient='records') if st.session_state.messages[-1]['sql_result'] is not None else None,
                'SQL_ERROR': st.session_state.messages[-1]['sql_error'],
                'PROMPT_TOKENS': st.session_state.messages[-1]['prompt_tokens'],
                'COMPLETION_TOKENS': st.session_state.messages[-1]['completion_tokens'],
                'AI_RESPONSE_TIME': st.session_state.messages[-1]['ai_response_time'],
                'QUERY_TIME': st.session_state.messages[-1]['query_time'],
                'FEEDBACK_SCORE': None,
                'FEEDBACK_TEXT': None,
                'USE_CASE': st.session_state.messages[-1]['use_case']}

            st.session_state.chat_history.append(chat_history_entry)
            write_data_to_sf(pd.DataFrame([chat_history_entry]), 'CHAT_HISTORY') # Write chat logs back to Snowflake 

    # Define scores for feedback widget
    score_mappings = {'😞':0, '🙁':0.25, '😐':0.5, '🙂':0.75, '😀':1}

    # Update chat history logs if user provides feedback
    if st.session_state.feedback:
        st.session_state.chat_history[-1]['FEEDBACK_SCORE'] = score_mappings[st.session_state.feedback['score']]
        st.session_state.chat_history[-1]['FEEDBACK_TEXT'] = st.session_state.feedback['text']
        update_feedback(st.session_state.chat_history[-1]['FEEDBACK_SCORE'], st.session_state.chat_history[-1]['FEEDBACK_TEXT'], st.session_state.chat_history[-1]['QUESTION_ID'])
