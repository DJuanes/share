"""Clase para calcular el intervalo de confianza"""
# Creator: Agustina Maccio
# Project name: Proyectp PHIT - Intervalos de Confianza

import numpy as np
import pandas as pd
import statsmodels.api as sm
from mapie.quantile_regression import MapieQuantileRegressor
from mapie.regression import MapieRegressor
from matplotlib import pyplot as plt
from sklearn.linear_model import QuantileRegressor
from statsmodels.stats.diagnostic import het_white


class PredictionIntervals:
    """Clase que representa los intervalos de predicción."""

    # constructor method with all the instance variables for the class
    def __init__(self, model, x, y_obs, y_pred):

        self.model = model
        self.x = x
        self.y_obs = y_obs
        self.y_pred = y_pred
        self.loess = None
        self.ti = None

    def residuals(self, y_obs, y_pred):
        """_summary_

        Args:
            y_obs (_type_): _description_
            y_pred (_type_): _description_

        Raises:
            ValueError: _description_
        """

        try:
            residuos = y_obs.reshape(-1, 1) - y_pred.reshape(-1, 1)
        except ValueError as e:
            raise ValueError(
                "White test requires y_obs and y_pred arrays to have same shape"
            ) from e

        return residuos

    def white_test(self, exog, y_obs, y_pred):
        """White's Lagrange Multiplier Test for Heteroscedasticity.

        Parameters
        ----------
        exog : array_like
            The explanatory variables for the variance. Squares and interaction
            terms are automatically included in the auxiliary regression.
        y_obs: array_like
            The observed values of the response variable.
        y_pred: array_like
            The predicted values of the response variable by the model.

        Returns
        -------
        Dictionary:
            Lagrange Multiplier Statistic : float
            Lagrange Multiplier Statistic pvalue : float
            F-Statistic : float
             The f-statistic of the hypothesis that the error variance does not
            depend on x. This is an alternative test variant not the original
                LM test.
            F-pvalue : float
                The p-value for the f-statistic.
        """

        residuals = self.residuals(y_obs, y_pred)
        x = sm.add_constant(exog)
        white_test = het_white(residuals, x)
        labels = [
            "White Test Statistic",
            "White Test Statistic p-value",
            "F-Statistic",
            "F-Test p-value",
        ]
        results = dict(zip(labels, white_test))

        print(results)
        return results

    def residuals_fitted_plot(self, y_obs, y_pred):
        """_summary_

        Args:
            y_obs (_type_): _description_
            y_pred (_type_): _description_
        """

        residuals = self.residuals(y_obs, y_pred)
        plt.rcParams["figure.figsize"] = [10, 5]
        plt.suptitle("Residuals vs Fitted")
        plt.scatter(y_pred, residuals, s=5)
        plt.ylabel("Residuals")
        plt.xlabel("Fitted values")
        plt.show()

    def mapie_intervals(
        self, model, x, y_obs, method, alpha, homo=True, n_jobs=None
    ) -> pd.DataFrame:
        """_summary_

        Args:
            model (_type_): _description_
            x (_type_): _description_
            y_obs (_type_): _description_
            method (_type_): _description_
            alpha (_type_): _description_
            homo (bool, optional): _description_. Defaults to True.
            n_jobs (_type_, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: _description_
        """

        if homo:
            mapie_homo = MapieRegressor(estimator=model, method=method, n_jobs=n_jobs).fit(
                x, y_obs.ravel()
            )
            y_pred, y_pis = mapie_homo.predict(x, alpha=alpha)

        else:
            mapie_het = MapieQuantileRegressor(QuantileRegressor(solver="highs", alpha=0)).fit(
                x, y_obs.ravel(), random_state=1
            )
            y_pred, y_pis = mapie_het.predict(x)

        interval = pd.DataFrame(y_pred, columns=["Y_pred"])
        interval["LINF_mapie"] = y_pis[:, 0]
        interval["LSUP_mapie"] = y_pis[:, 1]
        interval["Interval_wide"] = abs(interval["LSUP_mapie"] - interval["LINF_mapie"])

        return interval

    def maronna_set_span(self, y_obs, y_pred, span, ylim=False):
        """MaronnaSetSpan usa la función Loess para ajustar los residuos del
           modelo en función de la estimación del modelo.

                " residuos_suavizados = Loess(y_pred) "

        Args:
            y_obs (pd.Series o np.array): Observaciones del modelo
            y_pred (_type_): Predicciónes del modelo
            span (float [0,1]): Ventana de suavizado para el ajuste de la
                            función Loess.
            ylim (float, optional): parámetro para ajustar la escala "y" del
                                    gráfico creado con el ajuste de los
                                    residuos. Defaults to None.
        """

        # se crean las variables x e y para el ajuste de Loess
        x_loess = np.array(y_pred).ravel()
        y_obs = np.array(y_obs).ravel()
        residuals = self.residuals(y_obs, x_loess)
        y_loess = (residuals.ravel()) ** 2

        # Ajuste de Loess function
        self.loess = sm.nonparametric.lowess(y_loess, x_loess, frac=span)
        smooth = self.loess[:, 1]
        smooth[smooth <= 0] = 0.0000001

        # normalización residuos y calculo de ti
        sigma_est = np.sqrt(smooth)
        self.ti = residuals / sigma_est.reshape(-1, 1)

        # Plot Loess ajuste
        plt.rcParams["figure.figsize"] = [10, 5]
        plt.suptitle(f"Span = {span}")
        plt.scatter(x_loess, y_loess, s=5)
        plt.plot(x_loess, smooth, color="red")
        plt.ylabel("Residuals")
        plt.xlabel("Fitted values")
        if ylim:
            plt.ylim((0, ylim))
        plt.show()

    def maronna_confidence_intervals(self, y_pred, alpha):
        """Función para calcular el intervalo de confianza

        Args:
            y_pred ( np.array): Predicción del modelo a la que se calculará el
                                intervalo de confianza.
            alpha (float): Parámetro para ajustar el intervalo de confianza

        Returns:
            pd.DataFrame: contiene la predicciond del modelo con el intervalo
                        de confianza inferior y superior.
        """

        # se formatea y_pred
        y_pred = np.array(y_pred).reshape(-1, 1)

        # Usando el resultado de Loess calculamos el error de normalizado
        # de la predicción
        sigma_est = np.sqrt(np.interp(y_pred, self.loess[:, 0], self.loess[:, 1])).reshape(-1, 1)

        # calculo ta y tb usando el alpha del intervalo de confianza
        ta = np.quantile(self.ti, alpha / 2)
        tb = np.quantile(self.ti, 1 - (alpha / 2))

        # Se calculan los intervalos de confianza
        linf = y_pred + (sigma_est * ta)
        lsup = y_pred + (sigma_est * tb)
        interval = pd.DataFrame(y_pred, columns=["y_pred"])
        interval["IC_INF"] = linf
        interval["IC_SUP"] = lsup

        return interval
