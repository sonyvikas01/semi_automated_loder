import pandas as pd
import streamlit as st
import pathlib
import os
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import snowflake.connector
import boto3
from PIL import Image
from streamlit.components.v1 import html
import js2py










st.markdown(f"""

    <label>
  <input type="radio" name="test" value="small" checked>
  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/b/bc/Amazon-S3-Logo.svg/1712px-Amazon-S3-Logo.svg.png" style="width:50px; hight:50px">
</label>

<label>
  <input type="radio" name="test" value="big">
  <img src="https://www.certificationcamps.com/wp-content/uploads/azurelogo-1.png" style="width:100px; hight:100px">
</label>


""", unsafe_allow_html=True)
st.markdown("""<script>
alert("Hello! I am an alert box!");

</script>""",unsafe_allow_html=True)