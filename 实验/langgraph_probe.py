"""Real LangGraph 1.0.5, no model, two sibling nodes, in-process checkpointer."""
from operator import add
from typing import Annotated, TypedDict
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, END, StateGraph


class State(TypedDict):
    value: Annotated[int, add]


def main():
    calls = {"A": 0, "B": 0}

    def node_a(state):
        calls["A"] += 1
        return {"value": 2}

    def node_b(state):
        calls["B"] += 1
        if calls["B"] == 1:
            raise RuntimeError("B stopped")
        return {"value": 4}

    g = StateGraph(State)
    for name, fn in (("A", node_a), ("B", node_b)):
        g.add_node(name, fn)
        g.add_edge(START, name)
        g.add_edge(name, END)
    saver = InMemorySaver()
    app = g.compile(checkpointer=saver)
    thread = {"configurable": {"thread_id": "lesson"}}
    try:
        app.invoke({"value": 1}, thread, durability="sync")
    except RuntimeError as e:
        if str(e) != "B stopped":
            raise
    snapshot = app.get_state(thread)
    raw = saver.get_tuple(thread)
    print("after failure:", calls, "visible state:", snapshot.values, "next:", snapshot.next)
    print("persisted writes (channel names):", sorted(w[1] for w in raw.pending_writes))
    assert calls == {"A": 1, "B": 1} and snapshot.next == ("B",) and snapshot.values["value"] == 3
    out = app.invoke(None, thread, durability="sync")
    print("after resume:", calls, "final:", out)
    assert calls == {"A": 1, "B": 2} and out["value"] == 7


if __name__ == "__main__":
    main()
