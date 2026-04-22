from __future__ import annotations

from pydantic import BaseModel, Field


class Bank(BaseModel):
    ogrn: str
    reg_number: str | None = None
    name: str
    license_status: str
    source_url: str
    reports_page_url: str


class ReportRef(BaseModel):
    ogrn: str
    bank_name: str
    reg_number: str | None = None
    form_name: str
    form_code: str | None = None
    report_date_raw: str
    report_date_iso: str | None = None
    report_url: str


class ParsedTable(BaseModel):
    table_index: int
    section: str | None = None
    headers: list[list[str]] = Field(default_factory=list)
    rows: list[list[str]] = Field(default_factory=list)
    raw_html: str


class ParsedReport(BaseModel):
    ogrn: str
    bank_name: str
    reg_number: str | None
    form_name: str
    form_code: str | None
    report_date_raw: str
    report_date_iso: str | None
    report_url: str
    title: str | None = None
    unit: str | None = None
    metadata: dict = Field(default_factory=dict)
    tables: list[ParsedTable] = Field(default_factory=list)
    raw_text: str = ""
    raw_html_snippet: str = ""
