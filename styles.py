params_basic = {
    "xtick.direction": "in",  # Ticks point inward
    "xtick.minor.visible": True,  # Show minor ticks
    "ytick.direction": "in",
    "ytick.minor.visible": True,
    "legend.frameon": False,  # Remove legend border
}

params_thin = {
    "xtick.major.size": 3,
    "xtick.major.width": 0.5,
    "xtick.minor.size": 1.5,
    "xtick.minor.width": 0.5,
    "ytick.major.size": 3,
    "ytick.major.width": 0.5,
    "ytick.minor.size": 1.5,
    "ytick.minor.width": 0.5,
    "axes.linewidth": 0.5,
    "grid.linewidth": 0.5,
    "lines.linewidth": 1.0,
}

params_serif = {
    **params_basic,
    **params_thin,
    "font.family": "serif",
    "font.serif": ["cmr10"],
    "axes.formatter.use_mathtext": True,
    "mathtext.fontset": "cm",
}

params_tex = {
    **params_basic,
    **params_thin,
    "text.usetex": True,
}