import streamlit as st
import os, sys, pandas

rpath = os.path.abspath('..')
if rpath not in sys.path:
    sys.path.insert(0, rpath)

from Dashboard.EDA import get_top_repliers

st.set_page_config(page_title='Data Analysis', page_icon = ":tada:", layout='wide')

with st.container():
    st.subheader("Hey!, I am Natnael :wave: ")
    st.title("Data Analytics dashboard for Slack Workspace")


# Call the function from the notebook to get top and bottom users
top_ten_reply_users =  get_top_repliers()   

df_top_repliers = pandas.DataFrame(top_ten_reply_users, columns=["index", "sender_name"])

# print(top_ten_reply_users.set_axis)
container = st.container()

with container:

    top_container = st.container()

    with top_container:
        # Create two columns within the container
        top_table_col, top_chart_col = st.columns(2)

        # Display top users table in the first column
        with top_table_col:
            st.subheader("Table - Top 10 Users by Reply Count")
            st.table(df_top_repliers)


        with top_chart_col:
            st.subheader("Bar Chart - Top 10 Users by Reply Count")
            st.bar_chart(df_top_repliers.set_index("index"))