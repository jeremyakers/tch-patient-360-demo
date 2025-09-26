"""
Content item models for chat message persistence.
Based on Cortex Agents example for proper content storage.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


@dataclass
class TextContentItem:
    """Text content item for messages."""
    text: str
    

@dataclass
class TableContentItem:
    """Table content item for displaying data."""
    data: List[List[Any]]
    columns: List[str]
    title: Optional[str] = None
    

@dataclass 
class ChartContentItem:
    """Chart content item for visualizations."""
    spec: Dict[str, Any]
    title: Optional[str] = None


@dataclass
class MessageContentItem:
    """Wrapper for different content types."""
    actual_instance: Union[TextContentItem, TableContentItem, ChartContentItem]


@dataclass
class ChatMessage:
    """Enhanced chat message with content persistence support."""
    role: str
    content: str = ""
    
    # Additional fields for rich content
    sql: Optional[str] = None
    thinking_steps: List[str] = field(default_factory=list)
    citations: List[Dict] = field(default_factory=list)
    results: Any = None
    chart_spec: Optional[Dict] = None
    
    # For content persistence
    processed_content: Optional[List[MessageContentItem]] = field(default=None)
    is_processed: bool = field(default=False)
    
    def store_processed_content(
        self, 
        processed_text: str,
        sql_query: Optional[str] = None,
        tables: List[Dict] = None,
        charts: List[Dict] = None
    ) -> None:
        """Store processed content for persistence across reruns."""
        processed_items = []
        
        # Thinking steps are stored separately in the ChatMessage object
        # They will be displayed in an expander in the UI
        
        # Add SQL query if present
        if sql_query:
            sql_text = f"### 🔍 Generated SQL Query\n```sql\n{sql_query}\n```"
            processed_items.append(
                MessageContentItem(actual_instance=TextContentItem(text=sql_text))
            )
        
        # Add tables if present
        if tables:
            for table_data in tables:
                processed_items.append(
                    MessageContentItem(
                        actual_instance=TableContentItem(
                            data=table_data['data'],
                            columns=table_data['columns'],
                            title=table_data.get('title')
                        )
                    )
                )
        
        # Add response text if present
        if processed_text:
            response_text = f"### 💬 Response\n{processed_text}"
            processed_items.append(
                MessageContentItem(actual_instance=TextContentItem(text=response_text))
            )
        
        # Add charts if present (after text, as agent says "chart below")
        if charts:
            for chart_data in charts:
                processed_items.append(
                    MessageContentItem(
                        actual_instance=ChartContentItem(
                            spec=chart_data['spec'],
                            title=chart_data.get('title')
                        )
                    )
                )
        
        self.processed_content = processed_items
        self.is_processed = True
    
    def get_display_content(self) -> List[MessageContentItem]:
        """Get content for display."""
        return self.processed_content or []
