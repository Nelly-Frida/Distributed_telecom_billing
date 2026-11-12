ROUTES = {
    ("POST", "/bill"):   "handle_bill",
    ("GET",  "/stats"):  "handle_stats",
    ("GET",  "/health"): "handle_health",
    ("GET",  "/export"): "handle_export",
    ("GET",  "/flush"):  "handle_flush",
}
