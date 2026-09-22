"""Event name normalization strategy."""

import re
from abc import ABC, abstractmethod


class NormalizationRule(ABC):
    """Abstract base for event normalization rules."""

    @abstractmethod
    def matches(self, venue: str) -> bool:
        """Check if this rule applies to the venue."""
        raise NotImplementedError

    @abstractmethod
    def normalize(self, venue: str) -> str:
        """Return the normalized event name."""
        raise NotImplementedError


class ExactMatchRule(NormalizationRule):
    """Rule for exact string matches (case-insensitive)."""

    def __init__(self, patterns: list[str], normalized_name: str):
        self.patterns = [p.lower() for p in patterns]
        self.normalized_name = normalized_name

    def matches(self, venue: str) -> bool:
        return venue.lower().strip() in self.patterns

    def normalize(self, venue: str) -> str:
        return self.normalized_name


class PatternMatchRule(NormalizationRule):
    """Rule for regex pattern matching."""

    def __init__(self, pattern: str, normalized_name: str):
        self.pattern = pattern
        self.normalized_name = normalized_name

    def matches(self, venue: str) -> bool:
        return bool(re.search(self.pattern, venue.lower().strip()))

    def normalize(self, venue: str) -> str:
        return self.normalized_name


class EventNormalizer:
    """Normalizes venue names to standardized event names."""

    def __init__(self):
        self.rules: list[NormalizationRule] = []
        self._register_default_rules()

    def _register_default_rules(self) -> None:
        self.rules.append(ExactMatchRule(["ccs", "acm ccs"], "ACM CCS"))
        self.rules.append(PatternMatchRule(r"asiaccs|asia[ -]?ccs", "ACM ASIA CCS"))
        self.rules.append(PatternMatchRule(r"euro.?s.?p", "IEEE EURO S&P"))
        self.rules.append(PatternMatchRule(r"ndss", "NDSS"))
        self.rules.append(PatternMatchRule(r"usenix security", "USENIX Security"))
        self.rules.append(PatternMatchRule(r"\bsp\b|symposium on security and privacy", "IEEE S&P"))
        self.rules.append(PatternMatchRule(r"hotnets", "HotNets"))
        self.rules.append(PatternMatchRule(r"sacmat", "ACM SACMAT"))
        self.rules.append(PatternMatchRule(r"acsac|computer security applications", "ACSAC"))
        self.rules.append(
            PatternMatchRule(r"csur|computing surveys|comput\. surv", "ACM Computing Surveys")
        )
        self.rules.append(
            PatternMatchRule(
                r"comst|communications surveys|commun\. surv",
                "IEEE Communications Surveys & Tutorials",
            )
        )
        self.rules.append(
            PatternMatchRule(
                r"fntsec|foundations and trends|found\. trends priv",
                "Foundations and Trends in Privacy and Security",
            )
        )
        self._register_additional_rules()

    def _register_additional_rules(self) -> None:
        """Security, networks, mobile, systems, and AI venues (canonical names match
        the area and tier registries)."""
        additional: list[tuple[str, str]] = [
            # Security
            (r"esorics|european symposium on research in computer security", "ESORICS"),
            (r"codaspy", "ACM CODASPY"),
            (r"\braid\b|research in attacks, intrusions", "RAID"),
            (r"\bcns\b|communications and network security", "IEEE CNS"),
            (r"wisec|wireless network security", "ACM WiSec"),
            (r"\bwoot\b|offensive technologies", "USENIX WOOT"),
            (r"satml|secure and trustworthy machine learning", "IEEE SaTML"),
            (r"aisec", "ACM AISec"),
            (r"trustcom", "TrustCom"),
            # Networks and systems
            (r"sigcomm", "ACM SIGCOMM"),
            (r"\bnsdi\b|networked systems design", "USENIX NSDI"),
            (r"\bimc\b|internet measurement conference", "ACM IMC"),
            (r"sigmetrics", "ACM SIGMETRICS"),
            (r"\batc\b|annual technical conference", "USENIX ATC"),
            (r"eurosys", "ACM EuroSys"),
            # Mobile
            (r"mobicom|mobile computing and networking", "ACM MobiCom"),
            (r"mobisys|mobile systems, applications", "ACM MobiSys"),
            (r"sensys|embedded networked sensor", "ACM SenSys"),
            (r"hotmobile", "ACM HotMobile"),
            # AI / ML
            (r"neurips|advances in neural information", "NeurIPS"),
            (r"\bicml\b|international conference on machine learning", "ICML"),
            (r"\biclr\b|learning representations", "ICLR"),
            (r"\baaai\b", "AAAI"),
            (r"\bijcai\b", "IJCAI"),
            (r"\bkdd\b|knowledge discovery and data mining", "ACM KDD"),
            (r"\bnaacl\b", "NAACL"),
            (r"\bacl\b|annual meeting of the association for computational", "ACL"),
            (r"\bemnlp\b|empirical methods in natural language", "EMNLP"),
        ]
        for pattern, name in additional:
            self.rules.append(PatternMatchRule(pattern, name))

    def normalize(self, venue: str) -> str:
        """Normalize venue name to standard event name."""
        if not venue:
            return venue

        normalized_venue = venue.lower().strip().replace("&amp;", "&")

        for rule in self.rules:
            if rule.matches(normalized_venue):
                return rule.normalize(normalized_venue)

        return venue

    def register_rule(self, rule: NormalizationRule) -> None:
        """Register a custom normalization rule."""
        self.rules.append(rule)
