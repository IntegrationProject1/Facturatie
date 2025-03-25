import mysql.connector

def delete_user_from_database(user_id):
    # Connect to the MySQL database
    conn = mysql.connector.connect(
        host="localhost",
        user="jouwuser",
        password="jouwwachtwoord",
        database="jouwdb"
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
