from langgraph.graph import END, StateGraph

from cap.chains.cardano.canon.query_normalizer import QueryNormalizer
from cap.services.agentic.agents import (
    AnswerAgent,
    CacheAgent,
    ContextAgent,
    CriticAgent,
    ExecutionAgent,
    PersistenceAgent,
    PlanningAgent,
    PostProcessingAgent,
)
from cap.services.agentic.state import AgenticQueryState
from cap.services.llm_client import LLMClient
from cap.services.redis_nl_client import RedisNLClient


def build_agentic_query_graph(
    llm_client: LLMClient,
    redis_client: RedisNLClient,
):
    cache_agent = CacheAgent(redis_client, "cache")
    planning_agent = PlanningAgent(llm_client, "plan")
    execution_agent = ExecutionAgent("execute")
    critic_agent = CriticAgent("critic")
    context_agent = ContextAgent("context")
    post_processing_agent = PostProcessingAgent("postprocess")
    answer_agent = AnswerAgent(llm_client, "answer")
    persistence_agent = PersistenceAgent(redis_client, "persitance")

    async def normalize_node(state: AgenticQueryState) -> AgenticQueryState:
        state["normalized_query"] = QueryNormalizer.normalize(state.get("user_query", ""))
        state.setdefault("retry_count", 0)
        state.setdefault("max_retries", 2)
        return state

    async def cache_node(state: AgenticQueryState) -> AgenticQueryState:
        return await cache_agent.run(state)

    async def planning_node(state: AgenticQueryState) -> AgenticQueryState:
        return await planning_agent.run(state)

    async def execution_node(state: AgenticQueryState) -> AgenticQueryState:
        return await execution_agent.run(state)

    async def critic_node(state: AgenticQueryState) -> AgenticQueryState:
        return await critic_agent.run(state)

    async def context_node(state: AgenticQueryState) -> AgenticQueryState:
        return await context_agent.run(state)

    async def post_processing_node(state: AgenticQueryState) -> AgenticQueryState:
        return await post_processing_agent.run(state)

    async def answer_node(state: AgenticQueryState) -> AgenticQueryState:
        return await answer_agent.run(state)

    async def persistence_node(state: AgenticQueryState) -> AgenticQueryState:
        return await persistence_agent.run(state)

    def after_cache(state: AgenticQueryState) -> str:
        return execution_agent.name if state.get("federated_query") else planning_agent.name

    def after_execute(state: AgenticQueryState) -> str:
        if state.get("infrastructure_limit_exceeded"):
            return answer_agent.name
        return critic_agent.name

    def after_critic(state: AgenticQueryState) -> str:
        result = state.get("execution_result")
        if result and result.has_data:
            return context_agent.name
        if state.get("federated_query") is None:
            return planning_agent.name
        return context_agent.name

    def after_answer(state: AgenticQueryState) -> str:
        if state.get("infrastructure_limit_exceeded"):
            return END
        return persistence_agent.name

    workflow = StateGraph(AgenticQueryState)

    workflow.add_node("normalize", normalize_node)
    workflow.add_node(cache_agent.name, cache_node)
    workflow.add_node(planning_agent.name, planning_node)
    workflow.add_node(execution_agent.name, execution_node)
    workflow.add_node(critic_agent.name, critic_node)
    workflow.add_node(context_agent.name, context_node)
    workflow.add_node(post_processing_agent.name, post_processing_node)
    workflow.add_node(answer_agent.name, answer_node)
    workflow.add_node(persistence_agent.name, persistence_node)

    workflow.set_entry_point("normalize")
    workflow.add_edge("normalize", cache_agent.name)
    workflow.add_conditional_edges(cache_agent.name, after_cache)
    workflow.add_edge(planning_agent.name, execution_agent.name)
    workflow.add_conditional_edges(execution_agent.name, after_execute)
    workflow.add_conditional_edges(critic_agent.name, after_critic)
    workflow.add_edge(context_agent.name, post_processing_agent.name)
    workflow.add_edge(post_processing_agent.name, answer_agent.name)
    workflow.add_conditional_edges(answer_agent.name, after_answer)
    workflow.add_edge(persistence_agent.name, END)

    return workflow.compile()
