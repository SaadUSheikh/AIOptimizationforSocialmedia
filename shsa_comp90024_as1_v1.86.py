

from mpi4py import MPI
import pandas as pd
import json
from datetime import datetime,timedelta

#VESRION 1.86 DATED 06-04-2024


# Function to process the twitter json file
def process_twitter_json_file(filename,rank,size):
    # Initialize dictionary to store twitter data
    sentiments_data = {}
    start_delimiter = '{"rows":[' # object of twitter json file used for processing
    end_delimiter = '{}]}\n' # End delimter to retain json format 
    line_index=0 # line number of each record in json file to parse the processing based on cpu core ranking
    line_rank_offset=rank+1 # the line number index mapped to cpu core ranking for processing 

    with open(filename, 'r',encoding='utf-8') as file:  # Open twitter file for processing
        for line in file: # Fetch each line in the json file
        
        
            if end_delimiter not in line: # Process the file if not end of line
            
                if line_index == line_rank_offset: # Process the file if it is mapped to cpu core ranking
                
                                data = json.loads(start_delimiter+line+end_delimiter) # Retain the twitter json object format of the line fetched by adding start and end delimiters 
                                timestamp=data.get("rows", [{}])[0].get("doc", {}).get("data", {}).get("created_at") # store the created_at key value in the variable of the record                          
                                sentiment=data.get("rows", [{}])[0].get("doc", {}).get("data", {}).get("sentiment")# store the sentiment key value in the variable of the record
                                if not isinstance(sentiment, (int, float)):                                
                                    sentiment=0 # set the value of sentiment key as '0' if it is not an integer or float as the file is found withe few records key value as None or dict                                
                                
                                dt_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") # Convert string to datetime object
                                    
                                # Extract year, month, day, and hour
                                year = dt_object.year
                                month = dt_object.month
                                day = dt_object.day
                                hour = dt_object.hour
                                
                                # Check if there is an existing row for the same year, month, day, and hour
                                key = (year, month, day, hour)
                                if key in sentiments_data:
                                     
                                        sentiments_data[key]['Total_Sentiments_Score']+= sentiment # Add the sentiment score to existing value
                                        sentiments_data[key]['count'] += 1 # Add the count of tweets to existing value                                       
                                        
                                else:
                                        sentiments_data[key] = {'Total_Sentiments_Score': sentiment,'count': 1} # Create the row for specific time stamp keys
                                        
                                line_rank_offset+=size    # Add the size to keep a check point for next line to be processed based on cpu core ranking              
            
            line_index+=1 # Increment the line index to check associated line that to be processed based on cpu core ranking
            
        # Convert the dictionary to a DataFrame and return
        df = pd.DataFrame([(key[0], key[1], key[2], key[3], value['count'], value['Total_Sentiments_Score']) for key, value in sentiments_data.items()],columns=['Year', 'Month', 'Day', 'Hour', 'Count', 'Total_Sentiments_Score'])
        #print(df)                
        return df                                        
                                        


def project_data_analysis(df):

# the happiest hour ever = Total sentiments aggregated on that hour
# the happiest day ever =Sum of all the sentiments for taht day 
# the most active hour ever = Total tweets aggregated on  that hour
# the most active day ever = Sum of all the tweets  for taht day 

    """
    This is first part of assignment to calculate  happiest hour ever
    """
    max_sentiment = df['Total_Sentiments_Score'].max()
    max_sentiment_year = df[df['Total_Sentiments_Score'] == max_sentiment]['Year'].iloc[0]
    max_sentiment_hour = df[df['Total_Sentiments_Score'] == max_sentiment]['Hour'].iloc[0]
    max_sentiment_month = df[df['Total_Sentiments_Score'] == max_sentiment]['Month'].iloc[0]
    max_sentiment_day = df[df['Total_Sentiments_Score'] == max_sentiment]['Day'].iloc[0]
    
    
    # Creating datetime objects for the happiest hour start and end
    happy_hour_start = datetime(year=max_sentiment_year, month=max_sentiment_month, day=max_sentiment_day, hour=max_sentiment_hour)
    happy_hour_end = happy_hour_start + timedelta(hours=1)

    # Handling the ordinal suffix for the day
    day_suffix = 'th' if 11 <= max_sentiment_day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(max_sentiment_day % 10, 'th')

    # Formatting the start hour and removing leading zero if present
    happy_hour_str_start = happy_hour_start.strftime('%I').lstrip('0')  # Hour in 12-hour clock possibly without leading 0

    # Formatting the end hour with AM/PM and removing leading zero, also including the day with ordinal suffix   
    #on Linux we can leave lstrip as this is comnig on windows so i added it 
    happy_hour_str_end = happy_hour_end.strftime('%I%p').lower().lstrip('0') + f" on {happy_hour_start.day}{day_suffix}" + happy_hour_start.strftime(' %B %Y')

    # Combining everything for the final output
    happy_hour_str = f"the happiest hour ever, {happy_hour_str_start}-{happy_hour_str_end} with an overall sentiment score of +{max_sentiment:.6f}"


    print('\n########################################################################################################\n')
    print(happy_hour_str)


    
    """
    This is end of first section for  happiest hour ever
    """





    """
    This is second  part of assignment to calculate happiest day ever
    """
    # Combine year, month, and day into a single date column for easier grouping
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    # Group by the new 'Date' column and calculate the max sentiment for each day
    daily_sentiment_max = df.groupby('Date')['Total_Sentiments_Score'].sum()

    # Find the day with the highest average sentiment
    happiest_day = daily_sentiment_max.idxmax()
    happiest_day_sentiment = daily_sentiment_max[happiest_day]

    # Handling the ordinal suffix for the day
    day_suffix = 'th' if 11 <= happiest_day.day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(happiest_day.day % 10, 'th')
    formatted_day_with_suffix = f"{happiest_day.day}{day_suffix}"

    # Combining everything for the final output
    happiest_day_str = f"the happiest day ever,{formatted_day_with_suffix} {happiest_day.strftime('%B %Y')} was the happiest day with an overall sentiment score of +{happiest_day_sentiment:.6f}"
    print(happiest_day_str)

    """
    This is end of second section for  happiest day ever
    """



    """
    This is third   part of assignment to calculate most active hour
    """
    # Calculate the most active hour
    # Extract the most active hour information
    # Extract the most active hour information
    most_active_hour_row = df.loc[df['Count'].idxmax()]
    most_active_hour_count = most_active_hour_row['Count']
    most_active_hour_year = most_active_hour_row['Year']
    most_active_hour_month = most_active_hour_row['Month']
    most_active_hour_day = most_active_hour_row['Day']

    # Creating datetime objects for the most active hour and its end
    most_active_hour_start_dt = datetime(year=most_active_hour_year, month=most_active_hour_month, day=most_active_hour_day, hour=most_active_hour_row['Hour'])
    most_active_hour_end_dt = most_active_hour_start_dt + timedelta(hours=1)

    # Handling the ordinal suffix for the day
    day_suffix = 'th' if 11 <= most_active_hour_day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(most_active_hour_day % 10, 'th')
    formatted_day_with_suffix = f"{most_active_hour_day}{day_suffix}"

    # Format the start and end hours, stripping leading zeros
    #on Linux we can leave lstrip as this is comnig on windows so i added it 
    most_active_hour_str_start = str(int(most_active_hour_start_dt.strftime('%I')))  # Converts hour to int to remove leading zero, then back to string
    most_active_hour_str_end = most_active_hour_end_dt.strftime('%I%p').lower()  # Keep '%I%p' for end hour and make lowercase
    most_active_hour_str_end = str(int(most_active_hour_str_end[:-2])) + most_active_hour_str_end[-2:]  # Remove leading zero from end hour


    # Combining everything for the final output
    most_active_hour_str = f"the most active hour ever,{most_active_hour_str_start}-{most_active_hour_str_end} on {formatted_day_with_suffix} {most_active_hour_start_dt.strftime('%B %Y')} had the most tweets (#{most_active_hour_count})"
    print(most_active_hour_str)
    
    

    """
    This is end of third section for  most active hour ever
    """



    """
    This is fourth  part of assignment to calculate most active day ever
    """
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])

    # Find the row with the maximum 'Count' aggregated by 'Date'
    most_active_day_row = df.groupby('Date')['Count'].sum().idxmax()
    most_active_day_count = df.groupby('Date')['Count'].sum().max()

    # Handling the ordinal suffix for the day
    day_suffix = 'th' if 11 <= most_active_day_row.day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(most_active_day_row.day % 10, 'th')
    formatted_day_with_suffix = f"{most_active_day_row.day}{day_suffix}"

    # Combining everything for the final output
    most_active_day_str = f"the most active day ever,{formatted_day_with_suffix} {most_active_day_row.strftime('%B %Y')} had the most tweets (#{most_active_day_count})"
    print(most_active_day_str)

    """
    This is end of fourth section for  most active day ever
    """
    
 





def main():

    # Timer start
    start_time = datetime.now()
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
  

    #twitter json file to be processed
    #filename = 'twitter-1mb.json'
    #filename = 'twitter-50mb.json'
    filename = 'twitter-100gb.json'


    
    
    # Gather dataframes from all processors
    local_df=process_twitter_json_file(filename,rank,size)
    global_dfs = comm.gather(local_df, root=0)
        
    if rank == 0:
                # Concatenate DataFrames from all processes into a list
                all_dfs = [df for df in global_dfs if df is not None]
                # Concatenate all DataFrames into one DataFrame
                combined_df = pd.concat(all_dfs, ignore_index=True)
                # Consolidate DataFrame based on columns 'Year','Month','Day','Hour', and aggregate by suming Count and Total_Sentiments_Score
                result_df = combined_df.groupby(['Year', 'Month','Day','Hour']).agg({'Count': 'sum','Total_Sentiments_Score': 'sum'}).reset_index()
                #print("\n",result_df)         
                
                project_data_analysis(result_df)

                # Print the process time for the job completion
                end_time = datetime.now()
                processing_time = (end_time - start_time).total_seconds()
                
                print('--------------------------------------------------------------------------------------------------------')
                print(f"Time taken for processing the job is: {processing_time:.3f} seconds")
                print('\n########################################################################################################\n')    
            
if __name__ == "__main__":
    main()