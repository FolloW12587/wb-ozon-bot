def escape_markdown(text: str) -> str:
    """
    Экранирует спецсимволы для Telegram Markdown (v1)
    """
    return (
        text.replace("\\", "\\\\")  # сначала экранируем обратный слеш
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("`", "\\`")
    )
