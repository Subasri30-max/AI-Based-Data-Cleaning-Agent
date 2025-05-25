import os
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel
from langchain_openai import OpenAI
from langgraph.graph import StateGraph, END

# Load environment variables
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

if not openai_api_key:
    raise ValueError("OPENAI_API_KEY is missing. Set it in .env or as an environment variable.")

# Instantiate the OpenAI model
llm = OpenAI(openai_api_key=openai_api_key, temperature=0)

# Define the state schema
class CleaningState(BaseModel):
    input_text: str
    structured_response: str = ""

# Define the AI Agent class
class AIAgent:
    def __init__(self):
        self.graph = self.create_graph()

    def create_graph(self):
        """Creates and returns a langgraph agent graph with state management"""
        graph = StateGraph(CleaningState)

        def agent_logic(state: CleaningState) -> CleaningState:
            """Processes input and returns a structured response"""
            response = llm.invoke(state.input_text)
            return CleaningState(input_text=state.input_text, structured_response=response)

        graph.add_node("cleaning_agent", agent_logic)
        graph.set_entry_point("cleaning_agent")
        graph.add_edge("cleaning_agent", END)

        return graph.compile()

    def process_data(self, df: pd.DataFrame, batch_size=20):
        """Processes data in batches to avoid OpenAI's token limit"""
        cleaned_responses = []

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i+batch_size]
            prompt = f"""
You are an AI data cleaning agent. Analyze the dataset:
{df_batch.to_string(index=False)}

- Identify missing values and choose the best imputation strategy (mean, mode, median)
- Remove duplicates
- Format text correctly
- Return the cleaned data as structured text.
"""
            state = CleaningState(input_text=prompt)
            result = self.graph.invoke(state)

            if isinstance(result, CleaningState):
                cleaned_responses.append(result.structured_response)
            elif isinstance(result, dict):
                # Just in case the result is a dict
                state_result = CleaningState(**result)
                cleaned_responses.append(state_result.structured_response)

        return "\n".join(cleaned_responses)
