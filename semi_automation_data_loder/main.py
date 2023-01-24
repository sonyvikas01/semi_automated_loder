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



st.set_page_config(page_title="Semi-Automated Data Accelerator")
hide_st_style = """
<style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
</style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

#connect to connect.params file
parm_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"connect.params")
param_file_path = pathlib.Path(parm_file)
parameters = {}
if param_file_path.exists():
    file_obj = open(parm_file)
    for line in file_obj:
        line = line.strip()
        if not line.startswith('#'):
            key_value = line.split('=')
            if len(key_value) == 2:
                parameters[key_value[0].strip()] = key_value[1].strip()
# logo
image = Image.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"boolean.png"))
st.image(image)

AWS = Image.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"aws.png"))
aws=st.image(AWS,width=50)
# st.markdown(f"""
# <img src="C:/UsersVikas Sony/Desktop/semi_automation_data_loder/aws.png" style="background:black;" >
# """,unsafe_allow_html=True)

AZUE = Image.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"azue.png"))
azue=st.image(AZUE,width=50)


GCP = Image.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"gcp.png"))
gcp=st.image(GCP,width=50)

_style = """
<style>
   
[data-testid="stImage"]{
position:relative;
right: 55%;
top:-50px;
width : 250px;
hight : 230px;
}     

</style>
            """
st.markdown(_style, unsafe_allow_html=True)

with st.container():
    
    snowflake_user = parameters['snowflake_user']
    snowflake_password = parameters['snowflake_password']
    snowflake_warehouse = parameters['snowflake_warehouse']
    snowflake_account = parameters['snowflake_account']
    aws_access_key_id = parameters['aws_access_key_id']
    aws_secret_access_key = parameters['aws_secret_access_key']
    bucket_name = parameters['bucket']

    con = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account
    )

    if con:
        st.success('Successfully connected to Snowflake')
    else:
        st.error('Failed to connect to Snowflake')

    # cloud selection

    st.markdown(f"""
    
    <label>
  <input type="radio" name="test" value="small" checked>
  <img src="https://xpert-careers.com/wp-content/uploads/2021/02/aws1.png" style="width:10px; hight:10px">
</label>

<label>
  <input type="radio" name="test" value="big">
  <img src="C:/Users/Vikas Sony/Desktop/semi_automation_data_loder/aws.png">
</label>""",unsafe_allow_html=True)
    # cloud = st.radio('**Select Cloud Provider**', ['AWS','Azure', 'GCP'],index=0,disabled=False,horizontal=True)
    types = st.radio('**Select File Type**', [st.image("https://xpert-careers.com/wp-content/uploads/2021/02/aws1.png"), 'AVRO', 'PARQUET'], index=0, disabled=False, horizontal=True)


    col1, col2 = st.columns(2)
    # Get a list of databases
    cursor = con.cursor()
    cursor.execute("SHOW DATABASES")
    databases = [row[1] for row in cursor]
    selected_database = st.selectbox('**Database**', databases)
    # Get a list of schemas in the current database
    cursor.execute("SHOW SCHEMAS")
    schemas = [row[1] for row in cursor]
    selected_schema = st.selectbox('**Schema**', schemas)

    # Select the database and schema
    cursor.execute(f"USE DATABASE {selected_database}")
    cursor.execute(f"USE SCHEMA {selected_schema}")

    url = URL(
        account=snowflake_account,
        user=snowflake_user,
        password=snowflake_password,
        database=selected_database,
        schema=selected_schema,
    )
    engine = create_engine(url)
    connection = engine.connect()

    # select file fomate

    upload = st.file_uploader(' ', type=[types])

    my_js = """
var links = window.parent.document.getElementsByTagName("input");
for(i = 0; i < links.length; i++) {
if(links[i].value ==1)
links[i].disabled=true

if(links[i].value ==2)
links[i].disabled=true

        }
            """

    my_html =f"<script>{my_js}</script>"
    html(my_html)
    if upload is None:
        st.error('Please Load The File')
    else:
        upload_file = pd.read_csv(upload)
        csv_string = upload_file.to_csv()
        st.write(upload_file)
        file_name = upload.name
        file = file_name.split('.')[0]
        res = ''.join([i for i in file if not i.isdigit()])
        file = res.rstrip('_')

        # load to cloud

        if cloud == 'AWS':
            s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            s3.put_object(Bucket=bucket_name, Key=str(file) + '/' + str(file_name), Body=csv_string)
            st.success("CSV file uploaded to S3 bucket!")

        
        colu1, colu2 = st.columns([1, 3])
        df = upload_file.columns
        # mygrid = make_grid(len(df), 2)
        position = []
        target_col = []
        ddf = []
        for x in range(len(df)):
            selected_input = colu1.checkbox(df[x], 'True')
            text = colu2.text_input('', df[x])
            if selected_input:
                ddf.append(text)
                target_col.append(df[x])
                position.append('$' + str(x + 1))

        st.markdown("""
            <style>
            [data-testid=column]:nth-of-type(1) [data-testid=stVerticalBlock]{
                gap: 4rem;
                padding-top: 40px;
            }
            </style>
            """, unsafe_allow_html=True)

        comma_sep_index_str = ",".join(position)
        selected_columns = upload_file.loc[:, target_col]

        #rename columns name
        for x in range(len(ddf)):
            selected_columns.rename(columns={target_col[x]: ddf[x]}, inplace=True)


        st.write(selected_columns)
        tables_=pd.read_sql("""show tables """, connection)
        stage_=pd.read_sql("""show stages """,connection)
        file_=pd.read_sql("""show file formats """, connection)
        pipe_=pd.read_sql("""show pipes """, connection)
        
        #check file_formate, pipe and

        check_tables=tables_['name'].tolist()
        check_file=file_['name'].tolist()
        check_stage= stage_['name'].tolist()
        check_pipe=pipe_['name'].tolist()

        auto = st.checkbox('Build the Automatic Pipe Line',True)
        cont = st.selectbox('Continue on Error', ['CONTINUE', 'ABORT_STATEMENT'])
        if cont=='ABORT_STATEMENT' :
            st.warning('Error File Treminates The Process')

        if st.button('Submit'):
            with engine.begin() as con:
                selected_columns.head(0).to_sql(file, con=con, if_exists='append', index=False)
            if f"{file.upper() }" in check_tables:
                st.error(f"Table {file } has Alredy Exist")
            else:
                st.success(f'Table {file} has been created Successfully')


            if f"{file.upper() + '_FILE_FORMAT'}" in check_file:
                st.error(f"{file + '_FILE_FORMAT'} Alredy Exist")

            else:

                pd.read_sql(f"""create file format {file + '_FILE_FORMAT'}
                        type=csv
                        field_delimiter=','
                         skip_header=1 """, connection)

            if f"{file.upper() + '_STAGE'}" in check_stage :
                st.error(f"{file + '_STAGE'} Alredy Exist")
            else:

                pd.read_sql(f"""
                    create stage {selected_database}.{selected_schema}.{file + '_STAGE'}
                    STORAGE_INTEGRATION = s3_int_semi_auto
                    url = 's3://{bucket_name}/{file}/'
                    file_format = {file + '_FILE_FORMAT'} """, connection)
            if f"{file.upper() + '_PIPE'}" in check_pipe :
                st.error(f"{file + '_PIPE'} Alredy Exist")
            else:

                pd.read_sql(f"""
                    create pipe {selected_database}.{selected_schema}.{file + '_PIPE'}
                    auto_ingest={auto}
                    as
                    copy into {selected_database}.{selected_schema}.{file}
                    from (select {comma_sep_index_str} from @{selected_database}.{selected_schema}.{file + '_STAGE'})
                    file_format = {file + '_FILE_FORMAT'}
                    pattern='.*{file}.*.csv'
                    on_error= {cont}
        
                    """, connection)
                pd.read_sql(f"alter pipe {file + '_PIPE'} refresh",connection)

                st.success(f"Successfully {file + '_pipe'} has been created !")
                
