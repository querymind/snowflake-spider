# Introduction 

This is a repo which includes script to run Spider SQL benchmark (https://yale-lily.github.io/spider) on Snowflake database. Details can be found on blogpost (https://medium.com/querymind/gpt-4s-sql-mastery-2cd1f3dea543)

# How to run Spider on Snowflake

## Install dependencies

```bash
pip install -r requirements.txt
```

## Import the data to snowflake

### Step 1. Create a database in snowflake

```sql
CREATE DATABASE IF NOT EXISTS spider;
```

This is optional, you can choose a different database name, but it is always good to not pollute the default database.

### Step 2. Update `spider_snowflake_importer.py`

Make sure the following variables are up-to-date

```python 
    snowflake_credentials = {
        "user": "...",
        "password": "...",
        "account": "...",
        "warehouse": "...",
        "database": "...",
        "role": "..."
    }

    # Set your base directory containing the SQLite databases
    base_directory = "path/to/spider/database"
```

Please note that the `base_directory` should point to the directory containing the Spider's databases after download:

```
academic/                         customers_and_invoices/           loan_1/                           scholar/
activity_1/                       customers_and_products_contacts/  local_govt_and_lot/               school_bus/
...
```

Step 3. Run the importer

```bash
python spider_snowflake_importer.py
```

## Run the validations

### Step 4. Update `spider_snowflake_validator.py`

Update Snowflake credentials like the previous step.
```python
snowflake_credentials = {
    "user": "...",
    "password": "...",
    "account": "...",
    "warehouse": "...",
    "database": "...",
    "role": "..."
}
```

You need to implement your own logic to get query from LLM model based on the question.

```python
# Implement your own fix_sql function, input an original_query, error_msg and output fixed_query
def fix_sql(schema, question: str, original_query, error_msg) -> str:
    raise NotImplementedError("fix_sql is not implemented yet")


# Implement your own gen_sql function, input a question, schema (which is part of Spider's database) and output a query
def gen_sql(schema, question: str) -> str:
    raise NotImplementedError("gen_sql is not implemented yet")
```

Some notes: 
- Schema is the spider's individual database name, for example `car_1`, `flight_1`, etc.
- Question is the spider's question, for example `How many singers do we have?`
- `fix_sql` will be called when the generated query has compilation error or execution error. You can implement your own logic to fix the query (via LLM or other approach).

Example of generate SQL query using OpenAI GPT. (It is just an example, you should add your own logic to get all tables DDL, etc. in order to let GPT or other LLM to generate it)

```python
def gen_sql(schema, question: str) -> str:
    # Generate SQL query using OpenAI GPT-3
    # You can use the following code to generate SQL query
    
    import openai
    tables = <get DDLs for all tables in the schema>
    prompt = f"SQL: {question}\nSchema: {tables}\n"
    response = openai.Completion.create(
        engine="...",
        prompt=prompt,
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0.6,
    )
    return response.choices[0].text
```

### Step 5. Run the validator

```bash
python spider_snowflake_validator.py --input_json=path/to/spider/dev.json --output_json=test-001.out
```

All the results will be saved in the file specified in `--output_json=`. 
Example output looks like  

```
 {
     "result": "failed",
     "error": "expected row ('Love', '2016') not found in actual rows",
     "expected_result": "['Song_Name', 'Song_release_year'][('Love', '2016')]",
     "actual_result": "['SINGER_NAME', 'SONG_RELEASE_YEAR'][('Tribal King', '2016')]",
     "generated_query": "SELECT ...",
     "db_id": "concert_singer",
     "question": "Show the name and the release year of the song by the youngest singer.",
     "expected_query": "SELECT song_name ,  song_release_year FROM singer ORDER BY age LIMIT 1",
     "elapsed_time": 39.745845079422
 },
 {
     "result": "succeeded",
     "generated_query": "SELECT ...",
     "expected_result": "['Song_Name', 'Song_release_year'][('Love', '2016')]",
     "actual_result": "['SONG_NAME', 'SONG_RELEASE_YEAR'][('Love', '2016')]",
     "db_id": "concert_singer",
     "question": "Show the name and the release year of the song by the youngest singer.",
     "expected_query": "SELECT song_name ,  song_release_year FROM singer ORDER BY age LIMIT 1",
     "elapsed_time": 24.116262912750244
 },
```

