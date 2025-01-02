import matplotlib.pyplot as plt #visualisointeja
import pandas as pd #funktioita


# Apufunktio minuuttien laskemiseen
def time_to_minutes(time):
    if pd.isna(time):  # Jos arvo on tyhjä (esim. "to" voi olla NaN)
        return 90  # Oletusarvoisesti pelaaja oli kentällä pelin loppuun asti
    minutes, seconds = map(int, time.split(':'))
    return minutes + seconds / 60

# Lisää sarakkeet minuuteille
positions['from_minutes'] = positions['from_time'].apply(time_to_minutes)
positions['to_minutes'] = positions['to_time'].apply(time_to_minutes)

# Laske peliaika minuuteissa
positions['duration'] = positions['to_minutes'] - positions['from_minutes']# Laske peliaika minuuteissa

# Ryhmittele pelaajan ja pelipaikan mukaan ja laske yhteen peliaika
total_duration = positions.groupby(['player_name', 'position'])['duration'].sum().reset_index()

# Nimeä sarakkeet selkeyden vuoksi
total_duration.columns = ['Player Name', 'Position', 'Total Minutes']


# tästä saisi kaikkien pelaajien minuutit näkyville
# Jötetty kommentiksi print(total_duration)












# Suodata pelipaikat, joissa on "midfield" (case-insensitive)
midfield_positions = positions[positions['position'].str.contains('midfield', case=False, na=False)]

# Suodata tietty pelipaikka (esim. "Forward")
# position_data = total_duration[total_duration['Position'] == 'Right Back']
position_data = total_duration[total_duration['Position'].str.contains('Midfield')]


# Luo pylväsdiagrammi
plt.bar(position_data['Player Name'], position_data['Total Minutes'])

# Lisää kaavion otsikko
plt.title('Playing Time for All Players in Forward Position')

# Aseta x-akselin otsikko
plt.xlabel('Player Name')

# Aseta y-akselin otsikko
plt.ylabel('Total Minutes')

# Käännä x-akselin tekstin suunta (jos pelaajien nimet ovat pitkiä)
plt.xticks(rotation=45, ha='right')
plt.subplots_adjust(left=0.3, bottom=0.5)

# Näytä kaavio
# Jätetty näytä kaavio kommentiksi plt.show()
