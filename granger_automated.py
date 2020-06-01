import numpy as np
import scipy.linalg
import scipy.stats as stats
from statsmodels.compat.python import (range, string_types)
from statsmodels.tools.tools import chain_dot
from statsmodels.tsa.tsatools import vec
from statsmodels.tsa.vector_ar import util
from statsmodels.tsa.vector_ar.hypothesis_test_results import \
    CausalityTestResults


def a_test_causality(self, caused, header, alpha, causing=None, kind='f'):
    self.names = header
    signif = alpha
    if not (0 < signif < 1):
        raise ValueError("signif has to be between 0 and 1")

    allowed_types = (string_types, int)

    if isinstance(caused, allowed_types):
        caused = [caused]
    if not all(isinstance(c, allowed_types) for c in caused):
        raise TypeError("caused has to be of type string or int (or a "
                        "sequence of these types).")
    caused = [self.names[c] if type(c) == int else c for c in caused]
    caused_ind = [util.get_index(self.names, c) for c in caused]

    if causing is not None:
        if isinstance(causing, allowed_types):
            causing = [causing]
        if not all(isinstance(c, allowed_types) for c in causing):
            raise TypeError("causing has to be of type string or int (or "
                            "a sequence of these types) or None.")
        causing = [self.names[c] if type(c) == int else c for c in causing]
        causing_ind = [util.get_index(self.names, c) for c in causing]

    if causing is None:
        causing_ind = [i for i in range(self.neqs) if i not in caused_ind]
        causing = [self.names[c] for c in caused_ind]

    k, p = self.neqs, self.k_ar
    # number of restrictions
    num_restr = len(causing) * len(caused) * p
    num_det_terms = self.k_exog

    # Make restriction matrix
    C = np.zeros((num_restr, k * num_det_terms + k ** 2 * p), dtype=float)
    cols_det = k * num_det_terms
    row = 0
    for j in range(p):
        for ing_ind in causing_ind:
            for ed_ind in caused_ind:
                C[row, cols_det + ed_ind + k * ing_ind + k ** 2 * j] = 1
                row += 1

    # Lutkepohl 3.6.5
    Cb = np.dot(C, vec(self.params.T))
    middle = scipy.linalg.inv(chain_dot(C, self.cov_params, C.T))

    # wald statistic
    lam_wald = statistic = chain_dot(Cb, middle, Cb)

    if kind.lower() == 'wald':
        df = num_restr
        dist = stats.chi2(df)
    elif kind.lower() == 'f':
        statistic = lam_wald / num_restr
        df = (num_restr, k * self.df_resid)
        dist = stats.f(*df)
    else:
        raise Exception('kind %s not recognized' % kind)

    pvalue = dist.sf(statistic)
    crit_value = dist.ppf(1 - signif)

    #       print(pvalue)
    #       print("---====--")
    return pvalue, CausalityTestResults(causing, caused, statistic,
                                        crit_value, pvalue, df, signif,
                                        test="granger", method=kind)


def Granger_automated(maxlag, model, lag_dic, output, result, header, alpha, index):
    # outer loop: different time lags
    # for t_lag in range(1, maxlag + 1):
    t_lag = maxlag
    print(t_lag)
    temp_p = 1
    temp_p_re = 1
    temp_lag = -1
    temp_lag_re = -1
    firstptr = 0
    end = len(header)
    # Fit VAR regression under current time lag
    results = model.fit(t_lag)
    while firstptr < end:
        secondptr = firstptr
        while secondptr < end:
            print("Start to test next pair\n")
            # test for B->A, reversed is A->B
            # note: vA = caused = effect
            name_variableA = str(header[firstptr])
            # note: vB = causing = cause
            name_variableB = str(header[secondptr])
            print("Check results in 'Results': Checking for {} can granger cause {}".format(name_variableB,
                                                                                            name_variableA))
            causality = results.test_causality(name_variableA, header, alpha, name_variableB, kind='f')
            print("Check results in 'Results_Reversed': Checking for {} can granger cause {}".format(name_variableA,
                                                                                                     name_variableB))
            causality_re = results.test_causality(name_variableB, header, alpha, name_variableA, kind='f')
            concat_pair_name = str(name_variableB + name_variableA)
            #                 print(concat_pair_name)
            concat_pair_name_re = str(name_variableA + name_variableB)

            # Causality Test
            if causality[0] < alpha:
                # Output causality result for this single test
                print("------------------------""Results""")
                print("{} Lag rejected H0, with p = {}".format(t_lag, causality[0]))
                # create lag_dic[t_lag]
                if t_lag not in lag_dic:
                    lag_dic[t_lag] = {}
                # print("lag_dic[t_lag] is")
                #                     print(lag_dic[t_lag])
                # save the current output p = causality[0] into the lag_dic[t_lag]
                if concat_pair_name not in lag_dic[t_lag]:
                    lag_dic[t_lag][concat_pair_name] = 1
                # temp_p is saved in lag_dic[concat_pair_name]["p"]
                if concat_pair_name not in lag_dic:
                    lag_dic[concat_pair_name] = {}
                    lag_dic[concat_pair_name]["lag"] = 0
                    lag_dic[concat_pair_name]["p"] = 1
                print("lag_dic [{}] [{}] is {}".format(t_lag, concat_pair_name, lag_dic[t_lag][concat_pair_name]))
                if causality[0] < lag_dic[t_lag][concat_pair_name]:
                    # save current p, which is lag_dic[t_lag][concat_pair_name] in this approach
                    lag_dic[t_lag][concat_pair_name] = causality[0]
                    # save the temp_p as smallest p
                    if lag_dic[t_lag][concat_pair_name] < lag_dic[concat_pair_name]["p"]:
                        lag_dic[concat_pair_name]["p"] = lag_dic[t_lag][concat_pair_name]
                        lag_dic[concat_pair_name]["lag"] = t_lag
                        #                         print(lag_dic[t_lag][concat_pair_name])
                        #                         print(lag_dic)
                        print("temp_lag for {} is {} ".format(concat_pair_name, lag_dic[concat_pair_name]["lag"]))
                        print("with temp_p as {} ".format(lag_dic[concat_pair_name]["p"]))
                        if not header[firstptr] == header[secondptr]:
                            output.append(
                                (header[firstptr], header[secondptr], temp_lag, lag_dic[t_lag][concat_pair_name], "GC",
                                 index))
                    else:
                        print("temp_p is not updated")
                        # g.edge(name_variableB, name_variableA, label=" {} ".format(lag_dic[concat_pair_name]["lag"]))
            else:
                print("H0 is not rejected in Results, go to test next pair")
            print("\n=========-------==========")

            if causality_re[0] < alpha:
                print("------------------------""Results_Reversed""")
                print("{} Lag rejected H0, with p = {}".format(t_lag, causality_re[0]))
                if t_lag not in lag_dic:
                    lag_dic[t_lag] = {}
                if concat_pair_name_re not in lag_dic[t_lag]:
                    lag_dic[t_lag][concat_pair_name_re] = 1
                # temp_p is saved in lag_dic[concat_pair_name_re]["p"]
                if concat_pair_name_re not in lag_dic:
                    lag_dic[concat_pair_name_re] = {}
                    lag_dic[concat_pair_name_re]["lag"] = 0
                    lag_dic[concat_pair_name_re]["p"] = 1
                print("lag_dic [{}] [{}] is {}".format(t_lag, concat_pair_name_re,
                                                       lag_dic[t_lag][concat_pair_name_re]))

                if causality_re[0] < lag_dic[t_lag][concat_pair_name_re]:
                    # save current p, which is lag_dic[t_lag][concat_pair_name_re] in this approach
                    lag_dic[t_lag][concat_pair_name_re] = causality_re[0]
                    # save the temp_p as smallest p
                    if lag_dic[t_lag][concat_pair_name_re] < lag_dic[concat_pair_name_re]["p"]:
                        lag_dic[concat_pair_name_re]["p"] = lag_dic[t_lag][concat_pair_name_re]
                        lag_dic[concat_pair_name_re]["lag"] = t_lag
                        print("temp_lag for {} is {} ".format(concat_pair_name_re,
                                                              lag_dic[concat_pair_name_re]["lag"]))
                        print("with temp_p as {} ".format(lag_dic[concat_pair_name_re]["p"]))
                        if not header[firstptr] == header[secondptr]:
                            output.append((header[secondptr], header[firstptr], temp_lag_re,
                                           lag_dic[t_lag][concat_pair_name_re], "GC", index))
                    else:
                        print("temp_p is not updated")
                        # g.edge(name_variableA, name_variableB, label=" {} ".format(lag_dic[concat_pair_name_re]["lag"]))
            else:
                print("H0 is not rejected in Results_Reversed, go to test next pair")
            print("\n=========-------==========")

            secondptr += 1
        firstptr += 1

        # print("********start to test next lag**********")
        # t_lag += 1
