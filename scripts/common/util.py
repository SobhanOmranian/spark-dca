
# color_default = "indigo"
# color_default = "dimgray"
# color_16c = "saddlebrown"
# color_8c = "seagreen"
#color_4c = "royalblue"

# color_default = "#940545"
# color_16c = "#E95C47"
# color_8c = "#56AEB0"
# color_4c = "#5E4FA3"
# color_2c = "darkgray"\

# color_64c = "g"
# color_default = "dimgray"
# color_16c = "#940545"
# color_8c = "#E95C47"
# color_4c = "#56AEB0"
# color_2c = "#5E4FA3"
import matplotlib.pyplot as plt
import pandas as pd
def setup(font_size, plot_style):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 800)
    plt.style.use(plot_style)
    plt.rcParams.update({'font.size': font_size})
    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')
    plt.rcParams['text.latex.preamble']=[r"\usepackage{graphicx} \newcommand*\pct{\protect\scalebox{0.5}{\%}}"]

def makeStringBold(txt):
    return r"\textbf{" + txt + "}"

def makeStringHuge(txt):
    return r"\Huge{" + txt + "}"

def makeStringhuge(txt):
    return r"\huge{" + txt + "}"

def makeStringLARGE(txt):
    return r"\LARGE{" + txt + "}"

def makeStringLarge(txt):
    return r"\Large{" + txt + "}"

def formatLabelForLatex(txt):
    return makeStringLarge(makeStringBold(txt))

setting_font_size_1 = "14"
setting_font_size_2 = "11"
setting_font_size_3 = "16"
setting_legend_font_size = "8"


color_static_bestfit = "#940545"
color_dynamic = "#56AEB0"

style_plot = "seaborn-ticks"
style_plot_alternative = "ggplot"


# VIRIDIS
color_64c = "g"
# color_default = "#480054"
color_default = "dimgray"
color_16c = "#435B8D"
color_8c = "#22A287"
color_4c = "#93DB35"
color_2c = "#F3E91C"
colors_viridis = [color_default, color_16c, color_8c, color_4c, color_2c]

# Inferno
color_64c = "g"
# color_default = "#480054"
color_default = "dimgray"
color_16c = "#300754"
color_8c = "#9E2864"
color_4c = "#EF7D15"
color_2c = "#F5FFA3"

# Black Body
color_64c = "g"
# color_default = "#480054"
color_default = "dimgray"
color_16c = "#B22222"
color_8c = "#E36905"
color_4c = "#EED214"
color_2c = "#F5FFA3"
color_stage_border = "black"

color_static_bestfit = colors_viridis[1]
color_dynamic = colors_viridis[2]

color_epoll_wait_time = "#D80014"
color_io_throughput = "#5400DF"
color_combined = "#197F48"


#blues
# color_default = "#cccccc"
# color_64c = "g"
# color_16c = "#003366"
# color_8c = "#336699"
# color_4c = "#6699cc"
# color_2c = "#99ccff"

#purple to yellow
# color_default = "#615961"
# color_64c = "#3f324f"
# color_16c = "#764476"
# color_8c = "#aa546a"
# color_4c = "#d1805c"
# color_2c = "#ebbe5e"

#blue to yellow
# color_default = "#514b4d"
# color_64c = "#070d59"
# color_16c = "#43374f"
# color_8c = "#7f6146"
# color_4c = "#bb8c3c"
# color_2c = "#f7b633"

#red to green
# color_default = "#423939"
# color_64c = "#681313"
# color_16c = "#6e152c"
# color_8c = "#252c96"
# color_4c = "#2b6fa1"
# color_2c = "#31ada2"