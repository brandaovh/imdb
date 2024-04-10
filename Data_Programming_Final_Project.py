#!/usr/bin/env python
# coding: utf-8

# # Data Programming Final Project
# 
# ## Overview
# This project involves fetching data from an API, storing it in MongoDB, and visualizing the data using Dash.

# ## Step 1: Connect to MongoDB

# In[2]:


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://brandaovh:654123@cluster0.qswfw8s.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# ## Step 2: Fetch Data from API

# In[3]:


# Importing necessary libraries
import requests  # Used for making HTTP requests
import pandas as pd  # Used for data manipulation and analysis
import time  # Used for time-related tasks

# Defining the API key and base URL for the movie database
api_key = '7e0def0d0f7c88164b052e522cbd2787'
url_base = 'https://api.themoviedb.org/3/discover/movie'

# Defining the maximum number of pages to fetch from the API
max_pages = 30

# Connect to MongoDB
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://brandaovh:654123@cluster0.qswfw8s.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

# Select a database and a collection to work with
db = client['imdb']
collection = db['imdb_movies']

# while True: NOTE THAT THIS WHILE TRUE IS THE ONE THAT ACTIVATES THE ONE HOUR LOOP
# AS INTENDED IN THIS EXERCISE IS ONLY TO SHOWCASE THE KNOWLEDGE BUT WE ARE NOT GOING TO EXECUTE IT.
    # Creating an empty list to store all the movies
# Defining the maximum number of pages to fetch from the API
max_pages = 30
# Creating an empty list to store all the movies
all_movies = []

# Looping over each page number
for page in range(1, max_pages + 1):
    # Constructing the URL for the current page
    url = f"{url_base}?api_key={api_key}&page={page}"
    # Making a GET request to the API
    response = requests.get(url)
    # If the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the response as JSON
        data = response.json()
        # Add the results (list of movies) to our list
        all_movies.extend(data['results'])
        # Wait for 0.2 seconds to avoid overwhelming the API (rate limiting)
        time.sleep(0.2)
    else:
        # If the request was not successful, print an error message and stop the loop
        print(f"Error en la solicitud: Código de estado {response.status_code}")
        break

# Connect to MongoDB
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://brandaovh:654123@cluster0.qswfw8s.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

# Select a database and a collection to work with
db = client['imdb']
collection = db['imdb_movies']

# Insert the data into the collection
collection.insert_many(all_movies)

    # Wait for one hour  NOTE THAT THIS TIME.SLEEP IS THE ONE THAT RE ACTIVATES THE ONE HOUR LOOP
    #AS INTENDED IN THIS EXERCISE IS ONLY TO SHOWCASE THE KNOWLEDGE BUT WE ARE NOT GOING TO EXECUTE
    # OF COURSE INDENTATION WILL BE NEEDED FOR THE NEW LOOP
   # time.sleep(3600)


# ##Code to delete Duplicates in MongoDB Cloud
# 

# In[ ]:


from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient(uri)
db = client['movie_database']

# Define the aggregation pipeline
pipeline = [
    {
        "$group": {
            "_id": { "title": "$title", "release_date": "$release_date" },
            "dups": { "$push": "$_id" },
            "count": { "$sum": 1 }
        }
    },
    {
        "$match": { "count": { "$gt": 1 } }
    }
]

# Execute the aggregation
results = db.movies.aggregate(pipeline)

# Remove duplicates
for doc in results:
    doc['dups'].pop(0)
    db.movies.delete_many({ "_id": { "$in": doc['dups'] } })


# ##Step 3, making the WebApp with Dash & Plotly
# 

# In[5]:


import dash
from dash import html, dcc, Input, Output, State, dash_table
import pandas as pd
import plotly.express as px
import pymongo
from bson.objectid import ObjectId

# Connect to your MongoDB server
client = pymongo.MongoClient("mongodb+srv://brandaovh:654123@cluster0.qswfw8s.mongodb.net/?ssl=true&ssl_cert_reqs=CERT_NONE")
# Select the database and collection
db = client['imdb']
collection = db['imdb_movies']

# Define Layout of App and add server
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True)
server = app.server
app.layout = html.Div([
    html.H1('Web Application connected to a Live Database', style={'textAlign': 'center'}),
    # interval activated once/week or when page refreshed
    dcc.Interval(id='interval_db', interval=86400000 * 7, n_intervals=0),
    html.Div(id='mongo-datatable', children=[]),

    html.Div([
        html.Div(id='pie-graph', className='five columns'),
        html.Div(id='hist-graph', className='six columns'),
        html.Div(id='vote-average-graph', className='six columns')
    ], className='row'),
    dcc.Store(id='changed-cell')
])

# Display Datatable with data from Mongo database
@app.callback(Output('mongo-datatable', component_property='children'),
              Input('interval_db', component_property='n_intervals')
              )
def populate_datatable(n_intervals):
    # Convert the Collection (table) date to a pandas DataFrame
    df = pd.DataFrame(list(collection.find()))
    # Convert id from ObjectId to string so it can be read by DataTable
    df['_id'] = df['_id'].astype(str)
    # Convert genre_ids from array to string
    df['genre_ids'] = df['genre_ids'].apply(lambda x: ', '.join(map(str, x)))

    return [
       dash_table.DataTable(
    id='our-table',
    data=df.to_dict('records'),
    columns=[{'id': p, 'name': p, 'editable': False} if p == '_id'
             else {'id': p, 'name': p, 'editable': True}
             for p in df],
    page_size=10,  # Only display 10 rows at a time
    style_table={'overflowX': 'auto', 'maxHeight': '500px', 'overflowY': 'auto'}
)
,
    ]


# store the row id and column id of the cell that was updated
app.clientside_callback(
    """
    function (input,oldinput) {
        if (oldinput != null) {
            if(JSON.stringify(input) != JSON.stringify(oldinput)) {
                for (i in Object.keys(input)) {
                    newArray = Object.values(input[i])
                    oldArray = Object.values(oldinput[i])
                    if (JSON.stringify(newArray) != JSON.stringify(oldArray)) {
                        entNew = Object.entries(input[i])
                        entOld = Object.entries(oldinput[i])
                        for (const j in entNew) {
                            if (entNew[j][1] != entOld[j][1]) {
                                changeRef = [i, entNew[j][0]]
                                break
                            }
                        }
                    }
                }
            }
            return changeRef
        }
    }
    """,
    Output('changed-cell', 'data'),
    Input('our-table', 'data'),
    State('our-table', 'data_previous')
)

# Update MongoDB and create the graphs
@app.callback(
    Output("pie-graph", "children"),
    Output("hist-graph", "children"),
    Output("vote-average-graph", "children"),
    Input("changed-cell", "data"),
    Input("our-table", "data"),
)
def update_d(cc, tabledata):
    if cc is None:
        # Convert tabledata back to DataFrame
        df = pd.DataFrame(tabledata)

        # Sort by 'vote_count' and 'popularity' and keep top 5
        top_vote_count = df.sort_values('vote_count', ascending=False).head(5)
        top_popularity = df.sort_values('popularity', ascending=False).head(5)

        # Build the Plots
        pie_fig = px.pie(top_vote_count, values='vote_count', names='original_title', color_discrete_sequence=px.colors.sequential.RdBu)
        hist_fig = px.histogram(top_popularity, x='original_title', y='popularity')

        # Update layout for better visualization
        pie_fig.update_layout(title_text='Top 5 Most Voted Movies', title_x=0.5)
        hist_fig.update_layout(title_text='Top 5 Most Popular Movies', title_x=0.5, xaxis_title='Movie Title', yaxis_title='Vote Average')

        return dcc.Graph(figure=pie_fig), dcc.Graph(figure=hist_fig)

if __name__ == '__main__':
    app.run_server(debug=True)



