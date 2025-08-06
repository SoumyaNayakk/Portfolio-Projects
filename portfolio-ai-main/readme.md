# Adaptive Recommendation Chatbot with Vector Database



This repository contains an app  with a graphical user interface (GUI) that connects to a remote database hosted in Qdrant's Managed Cloud Service. The purpose of this  is to provide a starting point for building applications that interact with a Qdrant vector database.

## Features

- Graphical user interface (GUI) for easy interaction with the Qdrant vector database.
- Seamless integration with Qdrant's Managed Cloud Service.
- Simple setup process by cloning the repository and adding your credentials to the `.env` file.


## Prerequisites

Before using this app boilerplate, make sure you have the following:

- Google collab and postman account.
- An active Qdrant Managed Cloud Service account.
- Your Qdrant Managed Cloud Service credentials.


```

## Configuration

To configure the app to work with your Qdrant Managed Cloud Service account, you need to add your credentials to the `.env` file. Follow these steps:


```

2. Open the `.env` file in a text editor and provide your Qdrant Managed Cloud Service and OpenAI   credentials:

```plaintext
OPENAI_API_KEY=

QDRANT_HOST=
QDRANT_API_KEY=
QDRANT_COLLECTION_NAME=
```

## Usage
1. Run Intro_to_Qdrant to create and initialize vector store.
2. While running the rag_chat_bot, upload run.py
3. Run streamlit on google collab using these commands
!wget -q -O - ipv4.icanhazip.com
# This will return an IP address, e.g., 35.147.138.250
!streamlit run app.py & npx localtunnel --port 8501
# Click on the provided link to access the app
streamlit run app.py
The app will open in your default web browser with the user interface.

# Link to video explanation
https://drive.google.com/file/d/1nhRRALB8GxN9r6uPNgor4-Sx_POZx_KL/view?usp=sharing

## License

The code in this repository is available under the [MIT License](LICENSE)
