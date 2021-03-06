from typing import List
import datetime
import requests

from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.rcsetup import cycler
import pandas as pd

DATA_GOUV_2_OPEN = {
    "date": "date",
    "granularite": "granularite",
    "maille_code": "maille_code",
    "maille_nom": "maille_nom",
    "rea": "reanimation",
    "hosp": "hospitalises",
    "dchosp": "deces",
    "incid_hosp": "nouvelles_hospitalisations",
    "incid_rea": "nouvelles_reanimations",
    "conf": "cas_confirmes",
    "esms_dc": "deces_ehpad",
    "esms_cas": "cas_confirmes_ehpad",
    "source_url": "source_url",
}


def download_france_data() -> pd.DataFrame:
    """Download and merges data from OpenCovid19-fr and data.gouv.fr
    """
    oc19_file = "opencovid19-fr-chiffres-cles.csv"
    gouv_file = "data-gouv-fr-chiffres-cles.csv"
    oc19_url = "https://raw.githubusercontent.com/opencovid19-fr/data/master/dist/chiffres-cles.csv"
    gouv_url = (
        "https://www.data.gouv.fr/fr/datasets/r/f335f9ea-86e3-4ffa-9684-93c009d5e617"
    )
    # run requests to download and save the data
    myfile = requests.get(oc19_url)
    with open(oc19_file, "wb") as f:
        f.write(myfile.content)
    file = requests.get(gouv_url)
    with open(gouv_file, "wb") as f:
        f.write(file.content)
    # Load both csv into pandas
    data = pd.read_csv(oc19_file)
    data_gouv = pd.read_csv(gouv_file)
    # Fill in some of the metadata that is not present in the government data
    data_gouv["granularite"] = "pays"
    data_gouv["maille_code"] = "FRA"
    data_gouv["maille_nom"] = "France"
    data["source_nom"] = "Santé publique France Data"
    data_gouv[
        "source_url"
    ] = "https://www.data.gouv.fr/fr/datasets/r/f335f9ea-86e3-4ffa-9684-93c009d5e617"
    data_gouv.rename(DATA_GOUV_2_OPEN, axis="columns", inplace=True)
    return pd.concat((data, data_gouv), join="outer")


def enable_time_series_plot(
    in_df, timein_field="time", timeseries_field_out="date", date_format="%Y-%m-%d",
):
    """
    Small tool to add a field to a dataframe which can be used for time series
    plotting
    """
    if timeseries_field_out not in in_df.columns:
        # Drop the bad data row.
        in_df = in_df.loc[in_df[timein_field] != "2020-11_11", :]
        in_df[timeseries_field_out] = pd.to_datetime(
            in_df[timein_field], format=date_format
        )
    return in_df


def axis_date_limits(axs, min_date=None, max_date=None, format_date=None):
    if type(axs) != type(list()):
        axs = [axs]
    # Tailor axis limits
    for ax in axs:

        if not (max_date is None):
            ax.set_xlim(right=pd.to_datetime(max_date, format=format_date))
        if not (min_date is None):
            ax.set_xlim(left=pd.to_datetime(min_date, format=format_date))


def oc19_data_preparation(
    data,
    maille_code,
    rows=[
        "t",
        "deces",
        "deces_ehpad",
        "reanimation",
        "hospitalises",
        "nouvelles_reanimations",
        "nouvelles_hospitalisations",
    ],
    no_negatives=["deces", "deces_ehpad"],
):
    if maille_code == "FRA":
        fra = data.loc[
            (data["maille_code"] == maille_code)
            # & ((data["source_nom"] != "OpenCOVID19-fr") | (data.index > "2021-02-19"))
        ]
        rows = [*rows, "cas_confirmes"]

    elif (data["source_nom"] == "OpenCOVID19-fr").any():
        fra = data.loc[
            (data["maille_code"] == maille_code)
            & (data["source_nom"] == "OpenCOVID19-fr"),
        ]
    else:
        fra = data.loc[(data["maille_code"] == maille_code), :]

    region = fra["maille_nom"].unique()[0]
    fra = fra[rows]
    non_time_rows = [key for key in rows if key != "t"]
    # Fill nas
    for i, (ind, row) in enumerate(fra.iterrows()):
        for f in non_time_rows:
            if pd.isna(row[f]):
                if i > 0:
                    fra.loc[ind, f] = fra.iloc[i - 1, fra.columns.get_loc(f)]
                else:
                    fra.loc[ind, f] = 0.0
    fra = fra.groupby(["t"]).aggregate(
        {"t": "first", **{key: "max" for key in non_time_rows}}
    )
    fra = fra.set_index("t")
    for i in range(fra.shape[0] - 1, 0, -1):
        row_num = i
        for col in no_negatives:
            col_num = fra.columns.get_loc(col)
            val = fra.iloc[row_num, col_num]
            val_prev = fra.iloc[row_num - 1, col_num]
            if val < val_prev:
                fra.iloc[row_num - 1, col_num] = val
    # ajout des totaux par jours
    def par_jour(df):
        return df - [0, *(df[:-1])]

    fra["reanimation_cumul"] = fra["reanimation"].cumsum()
    non_time_rows.append("reanimation_cumul")
    fra["hospitalises_cumul"] = fra["hospitalises"].cumsum()
    non_time_rows.append("hospitalises_cumul")
    for f in non_time_rows:
        fra[f + "_jour"] = par_jour(fra[f])
        f += "_jour"
        fra[f + "_jour"] = par_jour(fra[f])
    # Ajout de entree + sortie vivante de reanimation
    fra["reanimation_solde_vivant_jour"] = fra["reanimation_jour"] + fra["deces_jour"]
    fra["reanimation_solde_vivant_jour_jour"] = par_jour(
        fra["reanimation_solde_vivant_jour"]
    )
    # Rolling averages
    for f in [*non_time_rows, "reanimation_solde_vivant"]:
        f = f + "_jour"
        fra[f + "_mma"] = fra[f].rolling(7).mean()
        f = f + "_jour"
        fra[f + "_mma"] = fra[f].rolling(7).mean()

    for f in [f"{v}_jour_mma" for v in no_negatives]:
        fra[f + "_jour"] = fra[f] - [0, *(fra[f][:-1])]

    for f in non_time_rows:
        fra[f + "_jour_prop"] = fra[f + "_jour_mma"] / fra[f]
    f = "reanimation_solde_vivant"
    fra[f + "_jour_prop"] = fra[f + "_jour_mma"] / fra["reanimation"]
    f = "deces_jour_mma"
    fra[f + "_jour_prop"] = fra[f + "_jour"] / fra["deces_jour_mma"]
    return fra, region


def oc19_data_preproc(
    data,
    maille_code,
    rows=["t", "deces", "deces_ehpad", "reanimation", "hospitalises"],
    no_negatives=["deces", "deces_ehpad"],
):
    fra, region = oc19_data_preparation(data, maille_code, rows, no_negatives)
    fig, axs = get_new_fig()
    plot_quants = [
        v
        for v in ["deces", "deces_ehpad", "reanimation", "reanimation_solde_vivant",]
        if v in rows
    ]
    for i, ext in enumerate(["_jour", "_jour_mma", "_jour_prop"]):

        fra.plot(
            y=[v + ext for v in plot_quants], ax=axs[i],
        )
        axs[i].grid(which="major")
        axs[i].set_title(region)
        axis_date_limits(axs[i], min_date="2020-03-01")
    return fra


def get_new_fig():
    fig, axs = plt.subplots(1, 3)
    fig.set_size_inches((15, 5))

    return fig, axs


def rol_val(df, list_rolls, **kwargs):
    if list_rolls:
        return (
            rol_val(df, list_rolls[:-1], **kwargs)
            .rolling(list_rolls[-1], **kwargs)
            .mean()
        )
    else:
        return df


def last_tuesday():
    return datetime.datetime.now() - datetime.timedelta(
        days=datetime.date.today().weekday() - 1
    )


def plot_field_loops(
    fra: pd.DataFrame,
    field: str,
    smoothing: List[int] = [7, 2, 3],
    maille_active="",
    start_date="2020-03-09",
    end_date=last_tuesday(),
    **kwargs,
):
    """Plots the day on day delta of a field of 'fra' against 

    Args:
        fra ([type]): [description]
        field ([type]): [description]
        smoothing (list, optional): [description]. Defaults to [7, 2, 3].
        maille_active (str, optional): [description]. Defaults to "".
        start_date (str, optional): [description]. Defaults to "2020-03-09".
        end_date ([type], optional): [description]. Defaults to last_tuesday().
    """
    smooth_rol_val = lambda df: rol_val(df, smoothing, **kwargs)

    fra[field + "_smooth_acceleration"] = smooth_rol_val(fra[field + "_jour_jour"])
    fra[field + "_jour_smooth"] = smooth_rol_val(fra[field + "_jour"])

    fra[field + "_smooth_acceleration_prop"] = (
        fra[field + "_smooth_acceleration"] / fra[field + "_jour_mma"]
    )

    # fig, axs = plt.subplots(1, 3)
    fig = plt.figure(constrained_layout=True)
    gs = fig.add_gridspec(nrows=3, ncols=2, wspace=0.3, hspace=0.35)
    axs = []
    axs.append(fig.add_subplot(gs[0, :]))
    axs.append(fig.add_subplot(gs[1:, 0]))
    axs.append(fig.add_subplot(gs[1:, 1]))
    fig.suptitle(f"Acceleration du nombre de {field} en {maille_active}")
    fig.set_size_inches(10, 8)
    colors = []
    for c in plt.rcParams["axes.prop_cycle"].by_key()["color"]:
        colors.append(c)
        colors.append(c)
    for ax in axs[:2]:
        ax.set_prop_cycle(cycler(color=colors))

    date_debut = pd.date_range(start=start_date, periods=1, freq="d")
    date_fin = end_date
    rolling_val = [1]

    rolled_fra = rol_val(fra, rolling_val)
    incr_val = date_debut[0].freq * 7
    while date_debut < date_fin:
        week_label = f'semaine du {date_debut[0].strftime("%d/%m")}'
        ind_log = (date_debut[0] <= rolled_fra.index) & (
            rolled_fra.index <= date_debut[0] + incr_val
        )
        # Timeline
        rolled_fra[ind_log].plot(
            y=field + "_jour",
            marker="o",
            linestyle="",
            ax=axs[0],
            label="",
            markersize=3,
        )
        rolled_fra[ind_log].plot(y=field + "_jour_smooth", ax=axs[0], label=week_label)
        # Timeline
        rolled_fra[ind_log].plot(
            x=field + "_jour_smooth",
            y=field + "_jour_jour",
            marker="o",
            ax=axs[1],
            label="",
            markersize=3,
            linestyle="",
        )
        rolled_fra[ind_log].plot(
            x=field + "_jour_smooth",
            y=field + "_smooth_acceleration",
            marker="+",
            ax=axs[1],
            label=week_label,
        )
        # Timeline
        rolled_fra[ind_log].plot(
            x=field + "_jour_smooth",
            y=field + "_smooth_acceleration_prop",
            marker="+",
            ax=axs[2],
            label=week_label,
        )
        date_debut += incr_val
    for ax in axs:
        lines = []
        for line in ax.get_legend().get_lines():
            if line.get_label():
                lines.append(line)
        leg = ax.legend(handles=lines, ncol=4)
        leg.set_bbox_to_anchor((0.9, -0.2))
    first_smooth = smoothing[:1]
    axs[0].set_ylabel(f"{field} par jour\n(moyenne sur {first_smooth} jours)")
    axs[0].grid("on")
    lines = []

    axs[1].set_xlabel(
        "{} par jour \n(moyenne sur {} jours)".format(field, first_smooth)
    )
    axs[1].set_ylabel("Delta journalier de l'abscisse ($x_t - x_{t-1}$)")
    axs[1].grid("on")

    axs[2].yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.0%}"))
    axs[2].grid("on")
    axs[2].set_ylabel("Delta proportionel journalier\nde l'abscisse")
    lims = axs[2].get_ylim()
    if lims[0] < -0.2 or lims[1] > 0.5:
        axs[2].set_ylim(-0.2, 0.5)
    axs[0].get_legend().remove()
    axs[1].get_legend().remove()
    axis_date_limits(
        axs[0],
        min_date=start_date,
        max_date=datetime.datetime.now().strftime("%Y-%m-%d"),
    )
    return axs


def state_tracking(
    timeseries: pd.DataFrame, time_state_start: int, time_state_end: int
):
    """Approximate the number of people in a given state.

    This is a very coarse approximation defined to track the number of people carrying
    the coronavirus disease.

    Args:
        timeseries : A pandas timeseries, or array like object.
        time_state_start : The number of days before a recorded event at which
            the state being approximated has started.
        time_state_end : the number of days after the recorded event at which the
            state being approximated ends.
    """
    delta_start = pd.to_timedelta(time_state_start, unit="days",)
    delta_end = pd.to_timedelta(time_state_end, unit="days",)

    state = timeseries.rolling(delta_end - delta_start, center=False).sum()
    state.index = state.index + delta_start

    return state


def plot_R_deaths(
    fra, data_series="deces_jour", offset=-10, windowing=None,
):
    return plot_R(
        fra,
        infectious_period_around_test=[-1, 2],
        infect_to_test_base=-5,
        data_series=data_series,
        windowing=windowing,
        offset=offset,
    )


def plot_R_hospitalised(
    fra, data_series="hospitalises", offset=-7, windowing=None,
):
    return plot_R(
        fra,
        infectious_period_around_test=[-1, 2],
        infect_to_test_base=-5,
        data_series=data_series,
        windowing=windowing,
        offset=offset,
    )


def plot_R_reanimation(
    fra, data_series="reanimation", offset=-7, windowing=None,
):
    return plot_R(
        fra,
        infectious_period_around_test=[-1, 2],
        infect_to_test_base=-5,
        data_series=data_series,
        windowing=windowing,
        offset=offset,
    )


def plot_R(
    fra,
    infectious_period_around_test=[-1, 2],
    infect_to_test_base=-5,
    windowing=None,
    data_series="cas_confirmes_jour",
    offset=0,
):
    if windowing is None:
        windowing = dict(window=7, center=True)
    fig, axs = plt.subplots(1, 2)
    fig.set_size_inches(10, 5)
    for sensitivity in [0, -1, 1]:
        infect_to_test = infect_to_test_base + sensitivity
        R, infectes, infectieux = calculate_R(
            fra[data_series],
            windowing=windowing,
            infectious_period_around_test=infectious_period_around_test,
            infect_to_data=infect_to_test,
            offset=offset,
        )
        if sensitivity == 0:

            infectious_time = (
                infectious_period_around_test[1] - infectious_period_around_test[0]
            )
            infectieux.plot(ax=axs[0], label="Personnes infectieuses")
            infectes.plot(ax=axs[0], label="Personnes infectées")
            (fra[data_series].rolling(**windowing).mean() * infectious_time).plot(
                ax=axs[0], label=f"{data_series} (x{infectious_time})"
            )
        R.plot(ax=axs[1], label=f"{-infect_to_test}")
    axs[0].legend()
    axs[1].legend().set_title("Days between infection\n and test")
    axs[1].set_ylim((0, 2))
    for ax in axs:
        ax.grid("on")
    return axs


def calculate_R(
    series: pd.Series,
    windowing: dict,
    infectious_period_around_test: List[int],
    infect_to_data: int,
    offset: int,
):
    """Calculates R for a series given 

    This function assumes that the infectious process is a Markov chain were the*
    number of infected peole on a day is:

    # infected = (R * # infectious) / (length of infectious period)

    Args:
        series : The series on which R needs to be measured
        windowing : smoothing to be applied to the data
        infectious_period_around_test : Infectious period as a number of days before
            and number of days after a test.
        infect_to_data : Time gap between infection and appearing in the data set
        offset:
    """
    infectieux = (
        state_tracking(
            series.rolling(**windowing).mean(), *infectious_period_around_test,
        )
        .rolling(**windowing)
        .mean()
    )
    infectious_time = (
        infectious_period_around_test[1] - infectious_period_around_test[0]
    )
    infectes = state_tracking(
        series.rolling(**windowing).mean(), infect_to_data, infect_to_data + 1,
    ).rolling(**windowing).mean() * (infectious_time)
    infectes.index = infectes.index + pd.to_timedelta(offset, unit="days")
    infectieux.index = infectieux.index + pd.to_timedelta(offset, unit="days")
    R = infectes / infectieux
    return R, infectes, infectieux
