from .tx_activity_heatmap import SCENE as tx_activity_heatmap
from .nft_mints_treemap_by_policy import SCENE as nft_mints_treemap_by_policy
from .gov_proposals_bubble_chart import SCENE as gov_proposals_bubble_chart
from .tx_fee_vs_value_scatter import SCENE as tx_fee_vs_value_scatter
from .current_trends_spacing_regression import SCENE as current_trends_spacing_regression
from .monthly_multiassets_2021 import SCENE as monthly_multiassets_2021
from .top_1pct_ada_supply import SCENE as top_1pct_ada_supply
from .monthly_tx_and_outputs import SCENE as monthly_tx_and_outputs

CHART_SCENES = {
    "tx_activity_heatmap": tx_activity_heatmap,
    "nft_mints_treemap_by_policy": nft_mints_treemap_by_policy,
    "gov_proposals_bubble_chart": gov_proposals_bubble_chart,
    "tx_fee_vs_value_scatter": tx_fee_vs_value_scatter,
    "current_trends_spacing_regression": current_trends_spacing_regression,
    "monthly_multiassets_2021": monthly_multiassets_2021,
    "top_1pct_ada_supply": top_1pct_ada_supply,
    "monthly_tx_and_outputs": monthly_tx_and_outputs,
}
