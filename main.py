# export PYSPARK_PYTHON=python3
import sys
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Blue(25)(4) Green(50)(3) Orange(75)(2) Red(100)(1)
killed_sum = 0
Injured_sum = 0
killed_signal = []
Injured_signal = []
state_name = []
ratio_ranking = []
percent_ranking = []
insurance_ranking_index = []

# print("Enter your state:")
# input_state = input()

# print("Enter your preferred type of vehicle: \n"
#       "1. Passenger Cars\n"
#       "2. Light Trucks\n"
#       "3. Large Trucks\n"
#       "4. Buses\n")
# type_of_vehicle = input()

if len(sys.argv) == 3:
    input_state = sys.argv[1]
    type_of_vehicle = sys.argv[2]
elif len(sys.argv) == 4:
    input_state = sys.argv[1] + ' ' + sys.argv[2]
    type_of_vehicle = sys.argv[3]

original_stdout = sys.stdout
with open('./txt/output_1.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('./csv/Fatalities and Fatality Rates.csv')
    col_head = []
    for i in range(len(df.columns)):
        col_head.append(df.dtypes[i][0])

    col_value = []
    for i in range(len(col_head)):
        col_value.append(df.select(col_head[i]).rdd.flatMap(lambda x: x).collect())
    # print(col_head)

    state_index = []
    for i in range(len(col_value[0])):
        if i >= 3:
            # print(col_value[0][i])  # US states
            state_index.append(i)  # US states index
    # print(state_index)

    for i in state_index:
        # Fatalities_count = int(col_value[6][i]) + int(col_value[7][i]) + int(col_value[8][i])
        Fatalities_rate = float(col_value[15][i]) + float(col_value[16][i]) + float(col_value[17][i])
        print("%s\t%f" % (col_value[0][i], Fatalities_rate / 3.0))
        # print("%s \t %f \t %f" % (col_value[0][i], Fatalities_count / 3.0, Fatalities_rate / 3.0))

    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./txt/output_2.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('./csv/death_report_state_by_state.csv')
    col_head = []
    for i in range(len(df.columns)):
        col_head.append(df.dtypes[i][0])
    # print(col_head)

    col_value = []
    for i in range(len(col_head)):
        col_value.append(df.select(col_head[i]).rdd.flatMap(lambda x: x).collect())
    # print(col_value)

    state_index = []
    for i in range(len(col_value[0])):
        if i >= 1:
            # print(col_value[0][i])  # US states
            state_index.append(i)  # US states index
        # print(state_index)

    for i in state_index:
        Fatal_accidents_ratio = ((float(col_value[1][i]) + float(col_value[2][i])) / float(col_value[3][i])) * 10000
        # car accident one ten thousandth
        print("%s\t%f" % (col_value[0][i], Fatal_accidents_ratio))

    # df2 = spark.read.csv('output_2.csv')
    # print(df2.show())
    # df.show()
    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./txt/output_3.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('./csv/Injures Vehicle Type.csv')
    col_head = []
    for i in range(len(df.columns)):
        col_head.append(df.dtypes[i][0])
    # print(col_head)

    col_value = []
    for i in range(len(col_head)):
        col_value.append(df.select(col_head[i]).rdd.flatMap(lambda x: x).collect())
    # print(col_value)

    vehicle_types = []  # Passenger Cars 2, Light Trucks 3, Large Trucks 4, Buses 5
    for i in range(len(col_value[0])):
        if 2 <= i <= 5:
            # print(col_value[0][i])  # US states
            vehicle_types.append(i)  # US states index
    # print(state_index)

    # print(col_value[1][37:46])
    for item in vehicle_types:
        killed_total = int(col_value[item][37]) + int(col_value[item][38]) + int(col_value[item][39]) + \
                       int(col_value[item][40]) + int(col_value[item][41]) + int(col_value[item][42]) + \
                       int(col_value[item][43]) + int(col_value[item][44]) + int(col_value[item][45])
        Injured_total = int(col_value[item][68]) + int(col_value[item][69]) + int(col_value[item][70]) + \
                        int(col_value[item][71]) + int(col_value[item][72]) + int(col_value[item][73]) + \
                        int(col_value[item][74]) + int(col_value[item][75]) + int(col_value[item][76])
        killed_sum = killed_sum + killed_total
        Injured_sum = Injured_sum + Injured_total
        print("%s \t %d \t %d" % (col_value[item][1], killed_total, Injured_total))

        # print(col_value[1][37])
        # print(col_value[1][45])

        # print(col_value[1][68])
        # print(col_value[1][76])

    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./output/feature_color.txt', 'w') as f:
    # with open('./output/by_state.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.text('./txt/output_1.txt')
    df_1 = spark.read.text('./txt/output_2.txt')
    df_2 = df.select(split(df.value, '\t')).alias('value').collect()  # ratio by state
    df_3 = df_1.select(split(df_1.value, '\t')).alias('value').collect()  # percentage
    # for item in range(len(df_2)):
    #     print("%s \t %s \t %s" % (df_2[item][0][0], df_2[item][0][1], df_3[item][0][1]))
    # print(df_2)
    # print(df_3)

    ratio_by_state_ranking = []
    percentage_ranking = []
    for item in range(len(df_2)):
        state_name.append(df_2[item][0][0])
        ratio_by_state_ranking.append(df_2[item][0][1])
        percentage_ranking.append(df_3[item][0][1])

    ratio_ranking = np.argsort(ratio_by_state_ranking)  # ascending
    percent_ranking = np.argsort(percentage_ranking)  # ascending

    # Blue(25) Green(50) Orange(75) Red(100)
    if ((int(np.where(ratio_ranking == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 25:
        print("Blue")
    elif ((int(np.where(ratio_ranking == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 50:
        print("Green")
    elif ((int(np.where(ratio_ranking == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 75:
        print("Orange")
    else:
        print("Red")
    if ((int(np.where(percent_ranking == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 25:
        print("Blue")
    elif ((int(np.where(percent_ranking == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 50:
        print("Green")
    elif ((int(np.where(percent_ranking == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 75:
        print("Orange")
    else:
        print("Red")

    # print(ratio_ranking)
    # print(percent_ranking)

    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./output/feature_color.txt', 'a') as f:
    # with open('./output/by_country.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.text('./txt/output_3.txt')
    df_2 = df.select(split(df.value, '\t')).alias('value').collect()
    for item in range(len(df_2)):
        # print("%s \t %s \t %s" % (df_2[item][0][0], df_2[item][0][1], df_2[item][0][2]))

        if (float(df_2[item][0][1]) / float(killed_sum)) * 100 <= 25:
            killed_signal.append("Blue")
        elif (float(df_2[item][0][1]) / float(killed_sum)) * 100 <= 50:
            killed_signal.append("Green")
        elif (float(df_2[item][0][1]) / float(killed_sum)) * 100 <= 75:
            killed_signal.append("Orange")
        else:
            killed_signal.append("Red")

        if (float(df_2[item][0][2]) / float(Injured_sum)) * 100 <= 25:
            Injured_signal.append("Blue")
        elif (float(df_2[item][0][2]) / float(Injured_sum)) * 100 <= 50:
            Injured_signal.append("Green")
        elif (float(df_2[item][0][2]) / float(Injured_sum)) * 100 <= 75:
            Injured_signal.append("Orange")
        else:
            Injured_signal.append("Red")

    print(killed_signal[int(type_of_vehicle) - 1])
    print(Injured_signal[int(type_of_vehicle) - 1])
    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./output/feature_color.txt', 'a') as f:
    # with open('./output/insurance_by_state.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('./csv/result_by_state auto_insurance_cost.csv')
    col_head = []
    for i in range(len(df.columns)):
        col_head.append(df.dtypes[i][0])

    col_value = []
    for i in range(len(col_head)):
        col_value.append(df.select(col_head[i]).rdd.flatMap(lambda x: x).collect())
    # print(len(col_value[0]))

    state_insurance_cost = []
    for i in range(len(col_value[0])):
        if i >= 1:
            state_insurance_cost.append(float(col_value[1][i]))

    insurance_ranking_index = np.argsort(state_insurance_cost)
    # for i in range(len(insurance_ranking_index)):
    #     print(col_value[0][insurance_ranking_index[i]+1])
    #     # insurance_ranking_index[i] + 1  # real index for each state ranking based on costs

    if ((int(np.where(insurance_ranking_index == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 25:
        print("Blue")
    elif ((int(np.where(insurance_ranking_index == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 50:
        print("Green")
    elif ((int(np.where(insurance_ranking_index == state_name.index(input_state))[0][0]) + 1) / 50.0) * 100 <= 75:
        print("Orange")
    else:
        print("Red")

    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./txt/output_4.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('./csv/Crash_Human_error.csv')
    col_head = []
    for i in range(len(df.columns)):
        col_head.append(df.dtypes[i][0])

    col_value = []
    for i in range(len(col_head)):
        col_value.append(df.select(col_head[i]).rdd.flatMap(lambda x: x).collect())
    # print(col_head)

    state_index = []
    for i in range(len(col_value[0])):
        if i >= 0:
            # print(col_value[0][i])  # US states
            state_index.append(i)  # US states index
    # print(state_index)

    # Blue(25) Green(50) Orange(75) Red(100)
    for i in state_index:
        Fatalities_count = float(col_value[19][i])
        Fatalities_rate = float(col_value[20][i])
        if Fatalities_rate <= 1.47:
            print("%s\tBlue" % col_value[0][i])
            # print("%s \t %f \t %f \t Blue" % (col_value[0][i], Fatalities_count, Fatalities_rate))
        elif 1.47 <= Fatalities_rate <= 2.945:
            print("%s\tGreen" % col_value[0][i])
            # print("%s \t %f \t %f \t Green" % (col_value[0][i], Fatalities_count, Fatalities_rate))
        elif 2.945 <= Fatalities_rate <= 4.4175:
            print("%s\tOrange" % col_value[0][i])
            # print("%s \t %f \t %f \t Orange" % (col_value[0][i], Fatalities_count, Fatalities_rate))
        elif Fatalities_rate >= 4.4175:
            print("%s\tRed" % col_value[0][i])
            # print("%s \t %f \t %f \t Red" % (col_value[0][i], Fatalities_count, Fatalities_rate))
    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./output/feature_color.txt', 'a') as f:
    # with open('./output/by_human_error.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.text('./txt/output_4.txt')
    df_1 = df.select(split(df.value, '\t')).alias('value').collect()  # color by state

    # print(state_name.index(input_state))
    # print(df_1[state_name.index(input_state)][0][0])
    print(df_1[state_name.index(input_state)][0][1])  # state by human error color
    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./txt/output_5.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('./csv/Crash_atmosphere.csv')
    col_head = []
    for i in range(len(df.columns)):
        col_head.append(df.dtypes[i][0])

    col_value = []
    for i in range(len(col_head)):
        col_value.append(df.select(col_head[i]).rdd.flatMap(lambda x: x).collect())
    # print(col_head)

    state_index = []
    for i in range(len(col_value[0])):
        if i >= 0:
            # print(col_value[0][i])  # US states
            state_index.append(i)  # US states index
    # print(state_index)

    # Blue(25) Green(50) Orange(75) Red(100)
    for i in state_index:
        Fatalities_count = float(col_value[19][i])
        Fatalities_rate = float(col_value[20][i])
        if Fatalities_rate <= 1.97:
            print("%s\tBlue" % col_value[0][i])
        elif 1.97 <= Fatalities_rate <= 3.945:
            print("%s\tGreen" % col_value[0][i])
            # print("%s \t %f \t %f \t green" % (col_value[0][i], Fatalities_count, Fatalities_rate))
        elif 3.945 <= Fatalities_rate <= 5.9175:
            print("%s\tOrange" % col_value[0][i])
            # print("%s \t %f \t %f \t orange" % (col_value[0][i], Fatalities_count, Fatalities_rate))
        elif Fatalities_rate >= 5.9175:
            print("%s\tRed" % col_value[0][i])
            # print("%s \t %f \t %f \t red" % (col_value[0][i], Fatalities_count, Fatalities_rate))

    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./output/feature_color.txt', 'a') as f:
    # with open('./output/by_atmosphere_by_state.txt', 'w') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.text('./txt/output_5.txt')
    df_1 = df.select(split(df.value, '\t')).alias('value').collect()  # color by state

    # print(state_name.index('California'))
    # print(df_1[state_name.index('California')][0][0])
    print(df_1[state_name.index(input_state)][0][1])
    sys.stdout = original_stdout

original_stdout = sys.stdout
with open('./output/final_color.txt', 'a+') as f:
    sys.stdout = f
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.text('./output/feature_color.txt')
    color = []
    for item in df.collect():
        if str(item[0]) == 'Blue':
            color.append(4)
        elif str(item[0]) == 'Green':
            color.append(3)
        elif str(item[0]) == 'Orange':
            color.append(2)
        elif str(item[0]) == 'Red':
            color.append(1)
    # print(color)
    final_weight = 0.0
    for item in color:
        final_weight = final_weight + item * (1 / len(color))
    # print(final_weight)
    # Blue(25)(4) Green(50)(3) Orange(75)(2) Red(100)(1)
    if 1.0 <= final_weight < 2.0:
        print(input_state + "\tRed\t" + str(final_weight))
    elif 2.0 <= final_weight < 3.0:
        print(input_state + "\tOrange\t" + str(final_weight))
    elif 3.0 <= final_weight < 4.0:
        print(input_state + "\tGreen\t" + str(final_weight))
    else:
        print(input_state + "\tBlue\t" + str(final_weight))
