from .base import StrategyTemplate, StrategyResult, ScreeningBounds
from .spot_paths import (
    S01_CexSpotToCexSpot,
    S02_CexSpotToDexSpot,
    S03_DexToDexSameChain,
    S04_ShortMultiHopSpot,
    S05_CrossChainInventory,
)
from .spot_perp import S06_LongSpotShortPerp, S07_ShortSpotLongPerp
from .perp_pairs import S08_LongPerpShortPerp
from .funding_events import S09_FundingEventCapture
from .hedge_switch import S10_HedgeTransfer

__all__ = [
    "StrategyTemplate",
    "StrategyResult",
    "ScreeningBounds",
    "S01_CexSpotToCexSpot",
    "S02_CexSpotToDexSpot",
    "S03_DexToDexSameChain",
    "S04_ShortMultiHopSpot",
    "S05_CrossChainInventory",
    "S06_LongSpotShortPerp",
    "S07_ShortSpotLongPerp",
    "S08_LongPerpShortPerp",
    "S09_FundingEventCapture",
    "S10_HedgeTransfer",
]
