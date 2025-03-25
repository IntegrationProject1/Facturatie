import mysql.connector
from dotenv import load_dotenv
import os

def delete_user_from_database(user_id):
    # Connect to the MySQL database
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
        # gaat moeten aangepast worden zodat credentials niet open zijn voor iedereen
    )
    
    cursor = conn.cursor()
    
    # Execute the delete statement
    cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
    
    # Commit the changes
    conn.commit()
    
    if cursor.rowcount > 0:
        print(f"User with ID {user_id} deleted from the database.")
    else:
        print(f"No user found with ID {user_id}.")
    
    # Close the cursor and connection
    cursor.close()
    conn.close()

# Example usage
delete_user_from_database(12345)  # Replace 12345 with the actual user ID you want to delete
