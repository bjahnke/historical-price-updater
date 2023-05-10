# Use the official Python 3.9 image as the base
FROM python:3.9

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the dependencies
RUN pip install -r requirements.txt

# Copy the rest of the application code to the working directory
COPY . .

# Set the command to run when the container starts
CMD [ "python", "your_script.py" ]
