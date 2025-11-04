"""
NotebookLM-Style Mind Map Generation using OpenAI GPT API
Generates structured overview mind maps from documents using OpenAI GPT models.
Includes a consistent Mermaid diagram in the response.
"""

import json
import time
from typing import List, Dict, Optional
from utils.settings import settings
from schemas.document import MindMapNode,MindMapResponse
from openai import OpenAI
from google.cloud.discoveryengine_v1 import (
    SearchServiceClient,
    SearchRequest,
)
from dotenv import load_dotenv
load_dotenv()

# Configuration
PROJECT_ID = settings.PROJECT_ID
LOCATION = settings.LOCATION
OPENAI_API_KEY=settings.OPENAI_API_KEY
if not OPENAI_API_KEY:
    raise ValueError(
        "OpenAI API key not found! Please set it using"
    )

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)










def get_document_content(
    project_id: str,
    location: str,
    engine_id: str,
    max_results: int = 10
) -> Dict[str, any]:
    client_search = SearchServiceClient()
    serving_config = (f"projects/{project_id}/locations/{location}/collections/default_collection/engines/{engine_id}/servingConfigs/default_search")
    query = ""
    request = SearchRequest(serving_config=serving_config, query=query, page_size=max_results)
    print(f" Retrieving documents for overview from engine: {engine_id}")
    try:
        response = client_search.search(request)
        documents, sources = [], set()
        for result in response.results:
            doc_data = result.document.derived_struct_data
            title = doc_data.get('title', 'Unknown Document')
            sources.add(title)
            content_pieces = []
            extractive_segments = doc_data.get('extractive_segments', [])
            if extractive_segments:
                for segment in extractive_segments:
                    content = segment.get('content', '').strip()
                    if content: content_pieces.append(content)
            extractive_answers = doc_data.get('extractive_answers', [])
            if extractive_answers:
                 for answer in extractive_answers:
                    content = answer.get('content', '').strip()
                    if content: content_pieces.append(content)
            if not content_pieces:
                for snippet in result.snippets:
                     snippet_text = snippet.snippet.strip().replace("<b>", "").replace("</b>", "")
                     if snippet_text: content_pieces.append(snippet_text)
            if content_pieces:
                unique_content = list(dict.fromkeys(content_pieces))
                documents.append({'title': title, 'content': ' '.join(unique_content)})
        print(f" Retrieved {len(documents)} document sections from {len(sources)} sources")
        return {'documents': documents, 'sources': list(sources), 'total_sections': len(documents)}
    except Exception as e:
        print(f" Error retrieving documents: {e}")
        raise


def generate_mindmap_with_openai(
    documents: List[Dict],
    max_depth: int,
    max_branches: int,
    model: str = "gpt-4o"
) -> Dict:
    print(f"\n Generating mind map with OpenAI {model}...")
    doc_context = "\n\n".join([f"## {doc['title']}\n{doc['content'][:1000]}" for doc in documents[:10]])
    
    prompt = f"""You are an expert at analyzing documents and creating structured mind maps.
Analyze the following documents and create a comprehensive overview mind map of ALL the content.

# Documents:
{doc_context}

# Task:
Create a hierarchical mind map that captures the main themes and structure:
- Maximum depth: {max_depth} levels
- Maximum branches per node: {max_branches}
- Identify the overarching themes and key concepts

# Instructions:
1. Identify the main themes across all documents
2. Extract key concepts and their relationships
3. Organize them in a clear hierarchy
4. Provide concise labels (3-7 words) for each node
5. Include brief descriptions (1-2 sentences)
6. Add 2-3 key points for important nodes

# Output Format:
Return ONLY valid JSON with this EXACT structure (no additional text):
{{
  "central_topic": "Main overarching theme (concise)",
  "branches": [
    {{
      "id": "1",
      "label": "Major theme (concise)",
      "description": "Brief description of this theme",
      "key_points": ["Key point 1", "Key point 2"],
      "level": 1,
      "children": [
        {{
          "id": "1.1",
          "label": "Sub-theme",
          "description": "Brief description",
          "key_points": ["Key point"],
          "level": 2,
          "children": []
        }}
      ]
    }}
  ]
}}

Remember: Return ONLY the JSON, no markdown formatting, no code blocks, no additional text."""

    try:
        print(f"   Analyzing documents with OpenAI {model}...")
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert at analyzing documents and creating structured mind maps. Always respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            top_p=0.8,
            max_tokens=8000,
            response_format={"type": "json_object"}
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Clean up any potential markdown formatting
        if result_text.startswith("```json"):
            result_text = result_text[7:]
        if result_text.startswith("```"):
            result_text = result_text[3:]
        if result_text.endswith("```"):
            result_text = result_text[:-3]
        
        mind_map_data = json.loads(result_text.strip())
        print(f" Mind map structure generated successfully")
        return mind_map_data
        
    except json.JSONDecodeError as e:
        print(f" Failed to parse JSON: {e}\nResponse: {result_text[:500]}")
        raise ValueError(f"Invalid JSON response from OpenAI: {e}")
    except Exception as e:
        print(f" OpenAI generation failed: {e}")
        raise


def flatten_mind_map_tree(
    branches: List[Dict],
    parent_id: Optional[str] = None
) -> tuple[List[MindMapNode], List[Dict[str, str]]]:
    nodes, relationships = [], []
    for branch in branches:
        node = MindMapNode(
            id=branch["id"],
            label=branch["label"],
            level=branch.get("level", 1),
            parent_id=parent_id,
            description=branch.get("description", ""),
            key_points=branch.get("key_points", [])
        )
        nodes.append(node)
        if parent_id:
            relationships.append({"from": parent_id, "to": branch["id"], "type": "contains"})
        if "children" in branch and branch["children"]:
            child_nodes, child_relationships = flatten_mind_map_tree(branch["children"], parent_id=branch["id"])
            nodes.extend(child_nodes)
            relationships.extend(child_relationships)
    return nodes, relationships


def create_mermaid_diagram(nodes: List[MindMapNode], relationships: List[Dict[str, str]]) -> str:
    """Converts nodes and relationships into Mermaid graph syntax."""
    mermaid_lines = ["graph TD"]
    for node in nodes:
        label = node.label.replace('"', "'")
        mermaid_lines.append(f'    {node.id}["{label}"]')
    for rel in relationships:
        mermaid_lines.append(f'    {rel["from"]} --> {rel["to"]}')
    return "\n".join(mermaid_lines)


def generate_mind_map(
    project_id: str,
    location: str,
    engine_id: str,
    model: str = "gpt-4o"
) -> MindMapResponse:
    """
    Generates a complete overview mind map, including a Mermaid diagram.
    """
    start_time = time.time()
    
    print(f"\n{'='*80}\n GENERATING OVERVIEW MIND MAP\n{'='*80}")
    print(f"Engine: {engine_id}\nMax Depth: 3 | Max Branches: 5 \nModel: {model}")
    
    try:
        doc_data = get_document_content(project_id=project_id, location=location, engine_id=engine_id, max_results=10)
    except Exception as e:
        raise ValueError(f"Failed to retrieve documents: {e}")
    
    if not doc_data['documents']:
        raise ValueError("No documents found in the engine for overview generation.")
    
    try:
        mind_map_data = generate_mindmap_with_openai(
            documents=doc_data['documents'],
            max_depth=3,
            max_branches=5,
            model=model
        )
    except Exception as e:
        raise ValueError(f"Failed to generate mind map with OpenAI: {e}")
    
    nodes, relationships = flatten_mind_map_tree(mind_map_data.get("branches", []))
    central_node = MindMapNode(
        id="0",
        label=mind_map_data.get("central_topic", "Document Overview"),
        level=0,
        description="Central concept"
    )
    
    for branch in mind_map_data.get("branches", []):
        relationships.insert(0, {"from": "0", "to": branch["id"], "type": "contains"})
    
    all_nodes = [central_node] + nodes
    
    mermaid_syntax = create_mermaid_diagram(all_nodes, relationships)
    
    generation_time = time.time() - start_time
    
    print(f"\n Mind map generation complete!\n   Total nodes: {len(all_nodes)}\n   Relationships: {len(relationships)}\n   Sources used: {doc_data['total_sections']}\n   Generation time: {generation_time:.2f}s\n{'='*80}\n")
    
    return MindMapResponse(
        title="Mind Map: Document Overview",
        central_topic=mind_map_data.get("central_topic", "Document Overview"),
        nodes=all_nodes,
        relationships=relationships,
        mermaid_diagram=mermaid_syntax,
        generation_time=generation_time,
        total_nodes=len(all_nodes),
        sources_used=len(doc_data['sources'])
    )
