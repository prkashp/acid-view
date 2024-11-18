import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import date
# from streamlit_dynamic_filters import DynamicFilters
from snowflake_connector import fetch_data
from processor import *
# from app.sidebar_obsolete import sidebar
DATA_TYPES = {'DATA_SOURCE': np.str_, 'PRIORITY': np.str_, 'CHECK_NAME': np.str_,'TABLE_NAME': np.str_,'STATUS':np.str_}
REMOTE_CSS_URL = "https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.4.1/semantic.min.css"
# SQL query path
st.set_page_config(page_title="Acid View", layout="wide", page_icon="./app/data/personify_health_brandmark_color.png", initial_sidebar_state="expanded")

def css():
    st.markdown(f'<link href="{REMOTE_CSS_URL}" rel="stylesheet">', unsafe_allow_html=True)
    try:
        with open("./app/style.css") as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except:
        raise Exception("File not found", os.getcwd(), os.listdir())

def table_cards(df):

    st.markdown("""
<style>
    [data-testid=stMarkdownContainer] {
        margin: 0;
    }
</style>
""", unsafe_allow_html=True)
    

    df_new = df[df['STATUS']=='Fail'].groupby(['TABLE_NAME','CHECK_NAME','DATA_SOURCE','TABLE_SCHEMA']).agg(COUNT=('TABLE_NAME', 'count'),START_DATE=('DATE','min'), LAST_DATE=('DATE','max')).sort_values('COUNT', ascending=False).reset_index()
    table_scorecard = """"""
    st.markdown("<h2>Top Table Failures</h2>", unsafe_allow_html=True)
    
    
    last_date = df_new.nlargest(1,'LAST_DATE','first')['LAST_DATE'].max()
    
    # st.values(last_date[0], df_new['LAST_DATE'])  
    # st.write(last_date)
    df_new = df_new[(df_new['LAST_DATE']==last_date) & (df_new['COUNT']>1)]
    table_scorecard = """<br><br><br><div id="mydiv" class="ui centered cards">"""
    # st.write(df_new)
    # df_new = df[['TABLE_NAME','DATA_SOURCE','TABLE_SCHEMA']].drop_duplicates()

    for index, row in df_new.iterrows():
        table_scorecard += """
<div class="card">   
    <div class=" content """+header_bg('BASE TABLE')+"""">
            <div class=" header smallheader">"""+str(row['TABLE_NAME'])+"""</div>
<div class="meta smallheader">"""+str(row['DATA_SOURCE'])+"."+str(row['TABLE_SCHEMA'])+"""</div>
</div>
<div class="extra content">
    <div class="meta"><i class="table icon"></i> Failure Count: """+str(row['COUNT'])+"""</div>
    <div class="meta"><i class="user icon"></i> Failure Type: """+str(row['CHECK_NAME'])+""" </div>
    <div class="meta"><i class="calendar alternate outline icon"></i> Failure started on:
        """+str(pd.to_datetime(row['START_DATE'], dayfirst=True, format='mixed').date())+"""</div>
    <div class="meta"><i class="calendar alternate outline icon"></i> Last Failure on:
        """+str(pd.to_datetime(row['LAST_DATE'], dayfirst=True, format='mixed').date())+"""</div>
</div>
</div>
        """
    # st.code(table_scorecard)
    st.markdown(table_scorecard, unsafe_allow_html=True)

def sidebar_v2(df):
    df_reset = df

    st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #003c44;
    }
    [data-testid=stSidebarUserContent] {
        padding-top: 2rem;
    }
    span[data-baseweb="tag"] {
        background-color: #ffcb12;
                color: #003c44;
    }
</style>
""", unsafe_allow_html=True)
    
    #init session_state
    if 'selectbox_date_range' not in st.session_state:
        st.session_state.selectbox_date_range = 0 
    if 'selectbox_table_name' not in st.session_state:
        st.session_state.selectbox_table_name = 1   
    if 'selectbox_data_source' not in st.session_state:
        st.session_state.selectbox_data_source = 2 
    if 'selectbox_priority' not in st.session_state:
        st.session_state.selectbox_priority = 3
    if 'selectbox_check_name' not in st.session_state:
        st.session_state.selectbox_check_name = 4
    if 'selectbox_status' not in st.session_state:
        st.session_state.selectbox_status = 5
    if 'selectbox_schema' not in st.session_state:
        st.session_state.selectbox_schema = 6
    
    # reset session_state
    def reset_button():
        st.session_state.selectbox_date_range += st.session_state.selectbox_date_range
        st.session_state.selectbox_table_name += st.session_state.selectbox_table_name   
        st.session_state.selectbox_data_source += st.session_state.selectbox_data_source
        st.session_state.selectbox_priority += st.session_state.selectbox_priority
        st.session_state.selectbox_check_name += st.session_state.selectbox_check_name
        st.session_state.selectbox_status += st.session_state.selectbox_status
        st.session_state.selectbox_schema += st.session_state.selectbox_schema

    # preprocess filter data
    col_table, col_db, col_schema, col_owner, col_table_type, col_status_type = preprocess_data(df)

    col_time_filter = {
            "Last 28 days" : 28,
            "Last 3 months" : 90,
            "Last 6 months" : 180,
            "Last 12 months" : 365
            }
    # TODO: Assign valid values to session state keys
    # Creating widgets
    selectbox_date_range = st.sidebar.selectbox(":clock3: Filter",tuple(col_time_filter.keys()), key=st.session_state.selectbox_date_range)
    selectbox_data_source = st.sidebar.multiselect("Source DB", col_db, col_db, key=st.session_state.selectbox_data_source)
    selectbox_schema = st.sidebar.selectbox("Schema", col_schema, index=len(col_schema)-1, key=st.session_state.selectbox_schema)
    selectbox_priority = st.sidebar.multiselect("Priority [In Progress]", col_owner,col_owner, key=st.session_state.selectbox_priority)
    selectbox_status = st.sidebar.multiselect("Status", col_status_type, col_status_type, key=st.session_state.selectbox_status)
    selectbox_check_name = st.sidebar.selectbox("Check Type", col_table_type, index=len(col_table_type)-1, key=st.session_state.selectbox_check_name)
    selectbox_table_name = st.sidebar.selectbox("Table Name", col_table, index=len(col_table)-1, key=st.session_state.selectbox_table_name)

    #Applying filters
    df = df[(df['DATE']>np.datetime64('today') - np.timedelta64(col_time_filter[selectbox_date_range],'D')) & (df['DATE']<np.datetime64('today'))]  
    df = df.loc[df["DATA_SOURCE"].isin(dedup(df["DATA_SOURCE"]))] if len(selectbox_data_source) <= 0 else df.loc[df["DATA_SOURCE"].isin(selectbox_data_source)]
    df = df.loc[df["TABLE_SCHEMA"].isin(all(df["TABLE_SCHEMA"]))] if selectbox_schema == "All" else df.loc[df["TABLE_SCHEMA"]==selectbox_schema]
    df = df.loc[df["PRIORITY"].isin(dedup(df["PRIORITY"]))] if len(selectbox_priority) <= 0 else df.loc[df["PRIORITY"].isin(selectbox_priority)]
    df = df.loc[df["STATUS"]].isin(dedup(df["STATUS"])) if len(selectbox_status) <= 0 else df.loc[df["STATUS"].isin(selectbox_status)]
    df = df.loc[df["CHECK_NAME"].isin(all(df["CHECK_NAME"]))] if selectbox_check_name == "All" else df.loc[df["CHECK_NAME"]==selectbox_check_name]
    df = df.loc[df["TABLE_NAME"].isin(all(df["TABLE_NAME"]))] if selectbox_table_name == "All" else df.loc[df["TABLE_NAME"]==selectbox_table_name]

    # st.write(selectbox_date_range,selectbox_data_source,selectbox_schema,selectbox_priority,selectbox_status,selectbox_check_name,selectbox_table_name)

    # Reset filter button
    reset = st.sidebar.button(label="Clear Selection", on_click=reset_button)
    df = df_reset if reset else df

    return df

def cards(df):
# .set_index('DATE').diff()
    # value = df.nlargest(2, 'DATE','first')
    
    df_summary=df.groupby(['DATE','STATUS'])['TABLE_NAME']\
                    .count()\
                    .reset_index()
    if len((df_summary[df_summary['STATUS']=='Pass']).nlargest(2,'DATE')['TABLE_NAME'].pct_change().index)>1:
        pass_pct = (df_summary[df_summary['STATUS']=='Pass']).nlargest(2,'DATE')['TABLE_NAME'].pct_change().iloc[1]
    else: 
        pass_pct = 0
    if len((df_summary[df_summary['STATUS']=='Fail']).nlargest(2,'DATE')['TABLE_NAME'].pct_change().index)>1:
        fail_pct = (df_summary[df_summary['STATUS']=='Fail']).nlargest(2,'DATE')['TABLE_NAME'].pct_change().iloc[1]
    else:
        fail_pct = 0
    col1,col2,col3 = st.columns([2,2,1], gap='large')
    col1.metric('Pass ✅', value=human_format((df['STATUS']=='Pass').sum()), delta=str(round(-pass_pct*100,2))+'%') # ☀️
    col2.metric("Fail ❌", value=human_format((df['STATUS']=='Fail').sum()), delta=str(round(-fail_pct*100,2))+'%', delta_color='inverse') # ⛈️
    col3.metric("Tables :1234:", value=human_format((df['TABLE_NAME'].nunique()))) # ☁️

def stacked_bar(df):
    bar = """
<div class="chart">
    <div class="y-axis">Count</div>"""
    
    
    df_chart=df[['DATE','STATUS','COUNT']].pivot(index='DATE', columns='STATUS', values='COUNT').reset_index()
    max_count = (df_chart['Fail']+df_chart['Pass']).max()
    df_chart['PCT']= (((df_chart['Fail']+df_chart['Pass'])/max_count)*100).round(2)
    # st.write(df_chart)

    for index, row in df_chart.iterrows():
        bar += """
    <div class="bar" style="--bar-height: """+str(row["PCT"])+"""%;">
        <div class="section" style="--section-value: """+str(row["Pass"])+""";" data-value="""+str(row["Pass"])+"""></div>
        <div class="section" style="--section-value: """+str(row["Fail"])+""";" data-value="""+str(row["Fail"])+"""></div>
        <div class="label">"""+str(row['DATE'].strftime("%d %b"))+"""</div>
    </div>"""

    bar += """
    <div id="chart-legend" class="panel-body">
        <div class="legend-value">
            <div class="legend-block" style="background-color: #39dd95;"></div>
                Pass
        </div>
        <div class="legend-value">
            <div class="legend-block" style="background-color: #e31e37;"></div>
                Fail
        </div>
    </div>
    <div class="x-axis">Date</div>
</div>"""
    # st.code(bar)
    st.markdown(bar, unsafe_allow_html=True)

def main():
    
    # st.title(":rainbow[MetaFlake View]")
    st.markdown("<h1>Acid View</h1>", unsafe_allow_html=True)

    css()
    # Fetch data from Snowflake
    data = None
    try: 
        # data = fetch_data()
        pass
    except:
        print('snowflake inaccessible')

    df = pd.read_csv('./app/data/data_validation_last_6m.csv', dtype=DATA_TYPES, parse_dates=['DATE']) if not data else data
    
    # sidebar_test()
    df_final = sidebar_v2(df)


    df_summary=df_final.groupby(['DATE','STATUS'])\
                    .agg(COUNT=('TABLE_NAME', 'count'))\
                    .reset_index()

    cards(df_final)
  

    color_dict = {"FAIL": "#e31e37", "WARN": "#fccb00", "PASS": "#39dd95"}
    fig = px.bar(df_summary, x='DATE', y='COUNT',
                 color="STATUS",
                color_discrete_map=color_dict,
                height=400)
    st.plotly_chart(fig)
    # stacked_bar(df_summary)
    table_cards(df_final)
    # st.write(df_summary)  

if __name__ == "__main__":
    main()
