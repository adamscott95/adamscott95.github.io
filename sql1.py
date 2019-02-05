# sql1.py
"""Volume 3: SQL 1 (Introduction).
Adam Robertson
Math 321
November 8, 2018
"""

import csv
import sqlite3 as sql
import numpy as np
from matplotlib import pyplot as plt

# Problems 1, 2, and 4
def student_db(db_file="students.db", student_info="student_info.csv",
                                      student_grades="student_grades.csv"):
    """Connect to the database db_file (or create it if it doesn’t exist).
    Drop the tables MajorInfo, CourseInfo, StudentInfo, and StudentGrades from
    the database (if they exist). Recreate the following (empty) tables in the
    database with the specified columns.

        - MajorInfo: MajorID (integers) and MajorName (strings).
        - CourseInfo: CourseID (integers) and CourseName (strings).
        - StudentInfo: StudentID (integers), StudentName (strings), and
            MajorID (integers).
        - StudentGrades: StudentID (integers), CourseID (integers), and
            Grade (strings).

    Next, populate the new tables with the following data and the data in
    the specified 'student_info' 'student_grades' files.

                MajorInfo                         CourseInfo
            MajorID | MajorName               CourseID | CourseName
            -------------------               ---------------------
                1   | Math                        1    | Calculus
                2   | Science                     2    | English
                3   | Writing                     3    | Pottery
                4   | Art                         4    | History

    Finally, in the StudentInfo table, replace values of −1 in the MajorID
    column with NULL values.

    Parameters:
        db_file (str): The name of the database file.
        student_info (str): The name of a csv file containing data for the
            StudentInfo table.
        student_grades (str): The name of a csv file containing data for the
            StudentGrades table.
    """
    # Problem 1
    try:
        with sql.connect(db_file) as conn:
            cur = conn.cursor()

            # Drop the four tables MajorInfo, CourseInfo, StudentInfo, StudentGrades
            cur.execute("DROP TABLE IF EXISTS MajorInfo;")
            cur.execute("DROP TABLE IF EXISTS CourseInfo;")
            cur.execute("DROP TABLE IF EXISTS StudentInfo;")
            cur.execute("DROP TABLE IF EXISTS StudentGrades;")

            # Create the four tables MajorInfo, CourseInfo, StudentInfo, StudentGrades with their schema
            cur.execute("CREATE TABLE MajorInfo (MajorID INTEGER, MajorName TEXT);")
            cur.execute("CREATE TABLE CourseInfo (CourseID INTEGER, CourseName TEXT);")
            cur.execute("CREATE TABLE StudentInfo (StudentID INTEGER, StudentName TEXT, MajorID INTEGER);")
            cur.execute("CREATE TABLE StudentGrades (StudentID INTEGER, CourseID INTEGER, Grade TEXT);")
    finally:
        conn.close() 

    # Problem 2
    try:
        with sql.connect(db_file) as conn:
            cur = conn.cursor()
            
            # Create lists of tuples for the four tables
            # Tuples for table MajorInfo
            major_info_tuples = [(1, "Math"), (2, "Science"), (3, "Writing"), (4, "Art")]
            # Tuples for table CourseInfo
            course_info_tuples = [(1, "Calculus"), (2, "English"), (3, "Pottery"), (4, "History")]
            # Tuples for table StudentInfo
            student_info_tuples = []
            with open("student_info.csv", 'r') as infile:
                student_info_tuples = list(csv.reader(infile))
            # Tuples for table StudentGrades
            student_grades_tuples = []
            with open("student_grades.csv", 'r') as infile:
                student_grades_tuples = list(csv.reader(infile))
            
            # Add tuples to corresponding tables using executemany
            cur.executemany("INSERT INTO MajorInfo VALUES(?,?);", major_info_tuples)
            cur.executemany("INSERT INTO CourseInfo VALUES(?,?);", course_info_tuples)
            cur.executemany("INSERT INTO StudentInfo VALUES(?,?,?);", student_info_tuples)
            cur.executemany("INSERT INTO StudentGrades VALUES(?,?,?);", student_grades_tuples)
            
            # Problem 4
            # Replace values of -1 in MajorID column of StudentInfo table with NULL
            cur.execute("UPDATE StudentInfo SET MajorID=NULL WHERE MajorID==-1;")
    finally:
        conn.close()
    with sql.connect("students.db") as conn:
        cur = conn.cursor()
        for row in cur.execute("SELECT * FROM StudentInfo;"):
            print(row)

# Problems 3 and 4
def earthquakes_db(db_file="earthquakes.db", data_file="us_earthquakes.csv"):
    """Connect to the database db_file (or create it if it doesn’t exist).
    Drop the USEarthquakes table if it already exists, then create a new
    USEarthquakes table with schema
    (Year, Month, Day, Hour, Minute, Second, Latitude, Longitude, Magnitude).
    Populate the table with the data from 'data_file'.

    For the Minute, Hour, Second, and Day columns in the USEarthquakes table,
    change all zero values to NULL. These are values where the data originally
    was not provided.

    Parameters:
        db_file (str): The name of the database file.
        data_file (str): The name of a csv file containing data for the
            USEarthquakes table.
    """
    # Problem 3
    try:
        with sql.connect(db_file) as conn:
            cur = conn.cursor()

            # Drop and Create table USEarthquakes
            cur.execute("DROP TABLE IF EXISTS USEarthquakes;")
            cur.execute("CREATE TABLE USEarthquakes (Year INTEGER, Month INTEGER, Day INTEGER, Hour INTEGER, Minute INTEGER, Second INTEGER, Latitude REAL, Longitude REAL, Magnitude REAL);")
            
            # Collect tuples for earthquakes from csv file
            earthquake_tuples = []
            with open("us_earthquakes.csv", 'r') as infile:
                earthquake_tuples = list(csv.reader(infile))
            
            # Insert tuples into table using executemany
            cur.executemany("INSERT INTO USEarthquakes VALUES(?,?,?,?,?,?,?,?,?);", earthquake_tuples)

            # Problem 4
            # Remove rows from USEarthquakes that have a value of 0 for the Magnitude
            cur.execute("DELETE FROM USEarthquakes WHERE Magnitude==0;")

            # Replace 0 values in Day, Hour, Minute and Second columns with NULL
            cur.execute("UPDATE USEarthquakes SET Day=NULL WHERE Day==0;")
            cur.execute("UPDATE USEarthquakes SET Hour=NULL WHERE Hour==0;")
            cur.execute("UPDATE USEarthquakes SET Minute=NULL WHERE Minute==0;")
            cur.execute("UPDATE USEarthquakes SET Second=NULL WHERE Second==0;")
    finally:
        conn.close()


# Problem 5
def prob5(db_file="students.db"):
    """Query the database for all tuples of the form (StudentName, CourseName)
    where that student has an 'A' or 'A+'' grade in that course. Return the
    list of tuples.

    Parameters:
        db_file (str): the name of the database to connect to.

    Returns:
        (list): the complete result set for the query.
    """
    try:
        with sql.connect(db_file) as conn:
            cur = conn.cursor()

            # Query the database and get all tuples of the form (StudentName, CourseName) where the student has an 'A' or 'A+' grade
            cur.execute("SELECT SI.StudentName, CI.CourseName "
                        "FROM StudentInfo AS SI, CourseInfo AS CI, StudentGrades AS SG "
                        "WHERE SI.StudentID==SG.StudentID AND (SG.GRADE=='A' OR SG.GRADE=='A+') " 
                        "AND CI.CourseID==SG.CourseID;")
            # Store tuples in a variable to preserve after closing the connection with the database
            tuples = cur.fetchall()
    finally:
        conn.close()
        return tuples


# Problem 6
def prob6(db_file="earthquakes.db"):
    """Create a single figure with two subplots: a histogram of the magnitudes
    of the earthquakes from 1800-1900, and a histogram of the magnitudes of the
    earthquakes from 1900-2000. Also calculate and return the average magnitude
    of all of the earthquakes in the database.

    Parameters:
        db_file (str): the name of the database to connect to.

    Returns:
        (float): The average magnitude of all earthquakes in the database.
    """
    try:
        with sql.connect(db_file) as conn:
            cur = conn.cursor()

            #Query the database and get all magnitudes of earthquakes during the 19th century (1800-1899)
            cur.execute("SELECT Magnitude FROM USEarthquakes WHERE Year >= 1800 AND Year <= 1899;")
            cent_19 = cur.fetchall()

            #Query the database and get all magnitudes of earthquakes during the 1-th century (1900-1999)
            cur.execute("SELECT Magnitude FROM USEarthquakes WHERE Year >= 1900 AND Year <= 1999;")
            cent_20 = cur.fetchall()

            #Query the database and get average of magnitudes of all earthquakes
            cur.execute("SELECT AVG(Magnitude) FROM USEarthquakes;")
            avg_all_quake = cur.fetchall()

            #Store results of queries in np arrays
            mags_19 = np.ravel(cent_19)
            mags_20 = np.ravel(cent_20)
            mags_avg = np.ravel(avg_all_quake)

            #plot histograms of earthquakes in 19th and 20th centuries
            ax1 = plt.subplot(121)
            ax1.hist(mags_19)
            ax1.set_title("19th Century Magnitudes")
            ax1.set_xlabel("Magnitude")
            ax1.set_ylabel("Frequency")
            
            ax2 = plt.subplot(122)
            ax2.hist(mags_20)
            ax2.set_title("20th Century Magnitudes")
            ax2.set_xlabel("Magnitude")
            ax2.set_ylabel("Frequency")
            
            plt.show()
    finally:
        conn.close()
        return np.mean(mags_avg[0])
