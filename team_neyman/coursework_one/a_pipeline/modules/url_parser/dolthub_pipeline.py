import subprocess
import pandas as pd
import io

def update_earnings_data(repo_path):
    subprocess.run(["dolt", "pull"], cwd=repo_path+"/earnings")
    
    query = """
    SELECT *
    FROM eps_history 
    """

    result = subprocess.run(
        ["dolt", "sql", "-q", query, "-r", "csv"], 
        cwd=repo_path, 
        capture_output=True, text=True
    )
    
    new_data = pd.read_csv(io.StringIO(result.stdout))

    return new_data
    