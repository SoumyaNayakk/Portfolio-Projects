#Calculating and Reporting Metrics of the RAG Pipeline


I executed this project using a RAG (Retrieval-Augmented Generation) pipeline to enhance the chatbot's ability to provide accurate and relevant responses. To evaluate the performance metrics of the RAG-based LLM, I utilized RAGAS, which offers a comprehensive set of tailored metrics. This approach allowed me to systematically measure and improve the chatbot's relevance, accuracy, and robustness in generated answers.

# Features

- Graphical user interface (GUI) for easy interaction with the Qdrant vector database.
- Seamless integration with Qdrant's Managed Cloud Service.
- Simple setup process by cloning the repository and adding your credentials to the `.env` file.
- Comes with a comprehensive set of requirements specified in the `requirements.txt` file.
- Performance evaluation is conducted based on ragas library and few custom functions

# Prerequisites

Before using this app boilerplate, make sure you have the following:

- Python 3.x installed on your system.
- An active Qdrant Managed Cloud Service account.
- Your Qdrant Managed Cloud Service credentials.

# Installation

To install and run the app boilerplate, follow these steps:

1. Clone this repository to your local machine:

```shell
git clone https://github.com/SoumyaNayakk/Dashboards/raga_application.git
```

2. Change into the project directory:

```shell
cd raga_application
```

3. Install the required Python packages using pip:


# Configuration

To configure the app boilerplate to work with your Qdrant Managed Cloud Service account, you need to add your credentials to the `.env` file. Follow these steps:

1. Copy the `.env.example` file and rename it to `.env`:

```shell
cp .env.example .env
```

2. Open the `.env` file in a text editor and provide your Qdrant Managed Cloud Service and OpenAI   credentials:

```plaintext
OPENAI_API_KEY=

QDRANT_HOST=
QDRANT_API_KEY=
QDRANT_COLLECTION_NAME=
```

# Usage
1. Run Intro_to_Qdrant to create and initialize vector store.
2. While running the 'raga updated', upload run.py
3. Run streamlit on google collab using these commands
!wget -q -O - ipv4.icanhazip.com
# This will return an IP address, e.g., 35.147.138.250
!streamlit run app.py & npx localtunnel --port 8501
# Click on the provided link to access the app
streamlit run app.py
The app will open in your default web browser with the user interface.
4. I have provided 2 text files containing the original code and its improved version. Copy both on app.py  and execute separately for comparative analysis

# Link to video explanation
https://drive.google.com/file/d/1dfBKuqJPcYqj-KgXSl6ZQ4wtBeR9-KuT/view?usp=sharing

## License

The code in this repository is available under the [MIT License](LICENSE)