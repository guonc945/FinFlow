# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set


SourceFieldNamesBuilder = Callable[[], Set[str]]
SourceFieldOptionsBuilder = Callable[[], List[Dict[str, str]]]
RelationLoader = Callable[[Any, Any], List[Dict[str, Any]]]


@dataclass(frozen=True)
class VoucherSourceModuleMeta:
    id: str
    label: str
    note: str = ""


@dataclass(frozen=True)
class VoucherSourceMeta:
    id: str
    module_id: str
    label: str
    source_type: str
    root_enabled: bool = False
    note: str = ""
    field_names_builder: Optional[SourceFieldNamesBuilder] = None
    field_options_builder: Optional[SourceFieldOptionsBuilder] = None


@dataclass(frozen=True)
class VoucherRelationMeta:
    resolver: str
    label: str
    root_source: str
    target_source: str
    loader: Optional[RelationLoader] = None


def build_source_modules_payload(
    modules: Dict[str, VoucherSourceModuleMeta],
    sources: Dict[str, VoucherSourceMeta],
) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for module_id, module_meta in modules.items():
        module_sources = []
        for source_meta in sources.values():
            if source_meta.module_id != module_id:
                continue
            field_options = source_meta.field_options_builder() if source_meta.field_options_builder else []
            module_sources.append(
                {
                    "id": source_meta.id,
                    "label": source_meta.label,
                    "source_type": source_meta.source_type,
                    "root_enabled": source_meta.root_enabled,
                    "note": source_meta.note,
                    "fields": field_options,
                }
            )

        payload.append(
            {
                "id": module_meta.id,
                "label": module_meta.label,
                "note": module_meta.note,
                "sources": module_sources,
            }
        )
    return payload


def build_relation_payload(relations: Dict[str, VoucherRelationMeta]) -> List[Dict[str, str]]:
    return [
        {
            "resolver": relation_meta.resolver,
            "label": relation_meta.label,
            "root_source": relation_meta.root_source,
            "target_source": relation_meta.target_source,
        }
        for relation_meta in relations.values()
    ]
