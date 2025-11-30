import sys
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
import logging

# Adicionar diretório scripts ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configurar console Rich com largura fixa para alinhamento
console = Console(width=100)

# Configurar logging com Rich - formato customizado
class CustomRichHandler(RichHandler):
    """Handler customizado com formatação alinhada."""
    
    def emit(self, record):
        """Override para formatação customizada."""
        # Formatar timestamp
        time_str = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        
        # Mapear níveis para ícones e cores
        level_styles = {
            "DEBUG": ("🔍", "dim white"),
            "INFO": ("ℹ", "bold blue"),
            "WARNING": ("⚠", "bold yellow"),
            "ERROR": ("❌", "bold red"),
            "CRITICAL": ("💥", "bold red on white"),
        }
        
        icon, color = level_styles.get(record.levelname, ("•", "white"))
        
        # Criar mensagem formatada
        message = self.format(record)
        
        # Formatar linha completa alinhada
        formatted = Text()
        formatted.append(f"[{time_str}] ", style="dim")
        formatted.append(f"{icon} ", style=color)
        formatted.append(f"[{record.levelname:8}] ", style=f"{color} dim")
        formatted.append(message, style="white")
        
        console.print(formatted)


def setup_logger(name: str = "manufatura", level: str = "INFO") -> logging.Logger:
    """
    Configura logger com Rich para output colorido e formatado.
    
    Args:
        name: Nome do logger
        level: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remover handlers existentes
    logger.handlers.clear()
    
    # Adicionar handler customizado
    custom_handler = CustomRichHandler(
        console=console,
        show_time=False,  # Vamos formatar manualmente
        show_path=False,
        rich_tracebacks=True,
        markup=True,
        show_level=False,  # Vamos formatar manualmente
    )
    
    formatter = logging.Formatter(
        "%(message)s"
    )
    custom_handler.setFormatter(formatter)
    logger.addHandler(custom_handler)
    
    return logger


def print_header(title: str, subtitle: str = None):
    """Imprime cabeçalho bonito e alinhado."""
    if subtitle:
        console.print(Panel.fit(
            f"[bold cyan]{title}[/bold cyan]\n[dim]{subtitle}[/dim]",
            border_style="cyan",
            box=box.ROUNDED,
            padding=(1, 2)
        ))
    else:
        console.print(Panel.fit(
            f"[bold cyan]{title}[/bold cyan]",
            border_style="cyan",
            box=box.ROUNDED,
            padding=(1, 2)
        ))
    console.print()  # Linha em branco


def print_summary_table(data: dict, title: str = "Resumo"):
    """Imprime tabela de resumo alinhada."""
    table = Table(
        title=title,
        show_header=True,
        header_style="bold cyan",
        box=box.ROUNDED,
        padding=(0, 1),
        min_width=60
    )
    table.add_column("Item", style="cyan", width=30, no_wrap=True)
    table.add_column("Valor", style="green", justify="right", width=30)
    
    for key, value in data.items():
        table.add_row(str(key), str(value))
    
    console.print(table)
    console.print()  # Linha em branco


def print_info(message: str):
    """Imprime mensagem INFO formatada."""
    time_str = datetime.now().strftime("%H:%M:%S")
    console.print(f"[dim]{time_str}[/dim] [bold blue]ℹ[/bold blue] [bold blue]INFO[/bold blue]     [white]{message}[/white]")


def print_success(message: str):
    """Imprime mensagem de sucesso formatada."""
    time_str = datetime.now().strftime("%H:%M:%S")
    console.print(f"[dim]{time_str}[/dim] [bold green]✅[/bold green] [bold green]SUCCESS[/bold green]  [white]{message}[/white]")


def print_warning(message: str):
    """Imprime mensagem de aviso formatada."""
    time_str = datetime.now().strftime("%H:%M:%S")
    console.print(f"[dim]{time_str}[/dim] [bold yellow]⚠[/bold yellow]  [bold yellow]WARNING[/bold yellow]  [white]{message}[/white]")


def print_error(message: str):
    """Imprime mensagem de erro formatada."""
    time_str = datetime.now().strftime("%H:%M:%S")
    console.print(f"[dim]{time_str}[/dim] [bold red]❌[/bold red] [bold red]ERROR[/bold red]    [white]{message}[/white]")


def print_step(step: int, total: int, message: str):
    """Imprime passo do processo formatado."""
    time_str = datetime.now().strftime("%H:%M:%S")
    console.print(f"[dim]{time_str}[/dim] [bold cyan]→[/bold cyan] [bold cyan]STEP[/bold cyan]     [white][{step}/{total}] {message}[/white]")


def print_separator(char: str = "─", length: int = 80):
    """Imprime separador visual."""
    console.print(f"[dim]{char * length}[/dim]")


# Logger padrão
logger = setup_logger()


